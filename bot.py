import discord
from discord import app_commands
from discord.ext import tasks
from discord.ui import View, Button
import os
import json
import re
import io
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import date, datetime, time, timedelta
from urllib.parse import unquote

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
N8N_WEBHOOK_URL = os.environ["N8N_WEBHOOK_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]
REMINDER_CHANNEL_ID = os.environ.get("REMINDER_CHANNEL_ID", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_DRIVE_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")
MAX_HISTORY = 20

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

# --- Google Drive セットアップ（オプション） ---
_drive_service = None


def get_drive_service():
    global _drive_service
    if _drive_service is not None:
        return _drive_service
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        return None
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        _drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
        print("[Drive] サービス初期化完了")
        return _drive_service
    except Exception as e:
        print(f"[Drive Init Error] {e}")
        return None


# Drive上のフォルダ構造（CLAUDE.mdと同じ）
DRIVE_FOLDER_CATEGORIES = {
    "事業計画": "01_事業計画",
    "内装": "02_改装計画",
    "改装": "02_改装計画",
    "行政": "03_行政資料",
    "消防": "03_行政資料",
    "法令": "04_法令・報酬",
    "報酬": "04_法令・報酬",
    "市場分析": "05_競合・市場",
    "競合": "05_競合・市場",
    "採用": "06_人事・採用",
    "人事": "06_人事・採用",
    "物件": "07_物件",
    "議事録": "03_行政資料",
    "研修": "06_人事・採用",
    "経理": "08_経理",
    "資金": "08_経理",
    "その他": "99_参考資料",
}

# DriveフォルダIDのキャッシュ（フォルダ名→ID）
_drive_subfolder_cache = {}


def get_or_create_drive_subfolder(subfolder_name):
    """共有ドライブ内のサブフォルダを取得。なければ作成。"""
    if subfolder_name in _drive_subfolder_cache:
        return _drive_subfolder_cache[subfolder_name]
    svc = get_drive_service()
    if not svc or not GOOGLE_DRIVE_FOLDER_ID:
        return GOOGLE_DRIVE_FOLDER_ID
    try:
        # 既存フォルダを検索
        query = (
            f"'{GOOGLE_DRIVE_FOLDER_ID}' in parents "
            f"and name = '{subfolder_name}' "
            f"and mimeType = 'application/vnd.google-apps.folder' "
            f"and trashed = false"
        )
        resp = svc.files().list(
            q=query, fields="files(id, name)",
            supportsAllDrives=True, includeItemsFromAllDrives=True, corpora="allDrives"
        ).execute()
        files = resp.get("files", [])
        if files:
            folder_id = files[0]["id"]
            _drive_subfolder_cache[subfolder_name] = folder_id
            return folder_id
        # なければ作成
        metadata = {
            "name": subfolder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [GOOGLE_DRIVE_FOLDER_ID],
        }
        result = svc.files().create(
            body=metadata, fields="id", supportsAllDrives=True
        ).execute()
        folder_id = result["id"]
        _drive_subfolder_cache[subfolder_name] = folder_id
        print(f"[Drive] フォルダ作成: {subfolder_name} ({folder_id})")
        return folder_id
    except Exception as e:
        print(f"[Drive Folder Error] {e}")
        return GOOGLE_DRIVE_FOLDER_ID


def resolve_drive_folder(category=None):
    """カテゴリからDrive上のサブフォルダIDを解決"""
    if not category:
        return GOOGLE_DRIVE_FOLDER_ID
    subfolder_name = DRIVE_FOLDER_CATEGORIES.get(category, "99_参考資料")
    return get_or_create_drive_subfolder(subfolder_name)


def upload_to_drive(file_bytes, filename, mime_type="application/octet-stream", category=None):
    """Driveにファイルをアップロード（カテゴリ別フォルダに振り分け）。"""
    svc = get_drive_service()
    if not svc or not GOOGLE_DRIVE_FOLDER_ID:
        return None
    try:
        from googleapiclient.http import MediaIoBaseUpload
        parent_id = resolve_drive_folder(category)
        media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype=mime_type, resumable=True)
        metadata = {"name": filename, "parents": [parent_id]}
        result = svc.files().create(
            body=metadata, media_body=media,
            fields="id, webViewLink, size, mimeType, name",
            supportsAllDrives=True
        ).execute()
        return {
            "file_id": result.get("id"),
            "web_link": result.get("webViewLink"),
            "size": int(result.get("size", 0)) if result.get("size") else 0,
            "mime_type": result.get("mimeType"),
            "name": result.get("name")
        }
    except Exception as e:
        print(f"[Drive Upload Error] {e}")
        return None


def download_from_drive(file_id):
    """Driveからファイルをダウンロード。戻り値: bytes or None"""
    svc = get_drive_service()
    if not svc:
        return None
    try:
        from googleapiclient.http import MediaIoBaseDownload
        request = svc.files().get_media(fileId=file_id, supportsAllDrives=True)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"[Drive Download Error] {e}")
        return None


def list_drive_folder_files(parent_id=None, recursive=True):
    """共有ドライブ/フォルダ内の全ファイルを取得（サブフォルダ再帰対応）"""
    svc = get_drive_service()
    if not svc or not GOOGLE_DRIVE_FOLDER_ID:
        return []
    if parent_id is None:
        parent_id = GOOGLE_DRIVE_FOLDER_ID
    try:
        query = f"'{parent_id}' in parents and trashed = false"
        all_items = []
        page_token = None
        while True:
            resp = svc.files().list(
                q=query,
                fields="nextPageToken, files(id, name, mimeType, size, webViewLink, modifiedTime, parents)",
                pageSize=100,
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                corpora="allDrives"
            ).execute()
            all_items.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        files = []
        for item in all_items:
            if item.get("mimeType") == "application/vnd.google-apps.folder":
                if recursive:
                    files.extend(list_drive_folder_files(item["id"], recursive=True))
            else:
                files.append(item)
        return files
    except Exception as e:
        print(f"[Drive List Error] {e}")
        return []


# Drive上のファイルのテキスト系判定
TEXT_FILE_EXTENSIONS = (".txt", ".md", ".csv", ".json", ".log", ".yaml", ".yml",
                         ".py", ".js", ".html", ".xml", ".tsv")


def is_text_file_meta(drive_file):
    """Driveのファイルメタデータからテキスト系か判定"""
    name = (drive_file.get("name") or "").lower()
    mime = drive_file.get("mimeType") or ""
    if mime.startswith("text/") or mime in ("application/json", "application/xml"):
        return True
    return name.endswith(TEXT_FILE_EXTENSIONS)


def fetch_drive_text_content(file_id, max_chars=20000):
    """Drive上のテキストファイルの中身を取得（UTF-8/Shift_JIS自動判定）"""
    data = download_from_drive(file_id)
    if not data:
        return None
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = data.decode("shift_jis")
        except UnicodeDecodeError:
            return None
    return text[:max_chars]


def delete_from_drive(file_id):
    svc = get_drive_service()
    if not svc:
        return False
    try:
        svc.files().delete(fileId=file_id, supportsAllDrives=True).execute()
        return True
    except Exception as e:
        print(f"[Drive Delete Error] {e}")
        return False


def rename_drive_file(file_id, new_name):
    """Drive上のファイルをリネーム"""
    svc = get_drive_service()
    if not svc or not file_id:
        return False
    try:
        svc.files().update(
            fileId=file_id,
            body={"name": new_name},
            supportsAllDrives=True,
        ).execute()
        return True
    except Exception as e:
        print(f"[Drive Rename Error] {e}")
        return False


def move_drive_file_to_folder(file_id, new_parent_id):
    """Drive上のファイルを別フォルダに移動"""
    svc = get_drive_service()
    if not svc or not file_id:
        return False
    try:
        # 現在の親フォルダを取得
        f = svc.files().get(
            fileId=file_id, fields="parents", supportsAllDrives=True
        ).execute()
        current_parents = ",".join(f.get("parents", []))
        svc.files().update(
            fileId=file_id,
            addParents=new_parent_id,
            removeParents=current_parents,
            supportsAllDrives=True,
        ).execute()
        return True
    except Exception as e:
        print(f"[Drive Move Error] {e}")
        return False


def is_mangled_filename(filename):
    """Discord等が日本語を削除してしまった名前かを判定"""
    import os as _os
    base = _os.path.splitext(filename)[0]
    # 日本語（ひらがな・カタカナ・漢字）があるならOK
    for ch in base:
        code = ord(ch)
        if 0x3040 <= code <= 0x30FF or 0x4E00 <= code <= 0x9FFF or 0xF900 <= code <= 0xFAFF:
            return False
    # ASCIIのみ＋短い（5文字以下）、もしくは先頭がアンダースコア
    if len(base) <= 5 or base.startswith("_"):
        return True
    return False


def infer_filename_from_message(message_text, original_filename):
    """メッセージ本文から意味のあるファイル名を推測"""
    import os as _os
    if not message_text:
        return original_filename
    first_line = message_text.strip().split("\n")[0]
    # ファイル名に使えない文字を除去
    clean = re.sub(r'[<>:"/\\|?*\r\n]', "", first_line).strip()
    # 末尾の句読点を除く
    clean = clean.rstrip("。、！？ .,!?")
    if not clean:
        return original_filename
    ext = _os.path.splitext(original_filename)[1] or ""
    new_name = clean[:80]
    if ext and not new_name.endswith(ext):
        new_name += ext
    return new_name


def get_db():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS conversation_history (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_conv_user_id
                ON conversation_history (user_id, created_at DESC)
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT,
                    status TEXT DEFAULT 'todo',
                    priority TEXT DEFAULT 'medium',
                    due_date DATE,
                    assignee_user_id TEXT,
                    created_by_user_id TEXT NOT NULL,
                    is_team_task BOOLEAN DEFAULT FALSE,
                    category TEXT,
                    external_id TEXT,
                    source TEXT DEFAULT 'discord',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_tasks_user
                ON tasks (created_by_user_id, status)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_tasks_team
                ON tasks (is_team_task, status)
            """)
            # external_id/sourceカラムが既存テーブルにない場合に追加
            for col, coltype, default in [
                ("external_id", "TEXT", None),
                ("source", "TEXT", "'discord'")
            ]:
                cur.execute(f"""
                    DO $$ BEGIN
                        ALTER TABLE tasks ADD COLUMN {col} {coltype}
                        {f"DEFAULT {default}" if default else ""};
                    EXCEPTION WHEN duplicate_column THEN NULL;
                    END $$
                """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schedules (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT,
                    schedule_type TEXT DEFAULT 'event',
                    start_date DATE NOT NULL,
                    start_time TIME,
                    end_date DATE,
                    end_time TIME,
                    location TEXT,
                    attendees TEXT,
                    is_team_event BOOLEAN DEFAULT FALSE,
                    category TEXT,
                    reminder_sent BOOLEAN DEFAULT FALSE,
                    external_id TEXT,
                    source TEXT DEFAULT 'discord',
                    created_by_user_id TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_schedules_date
                ON schedules (start_date, start_time)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_schedules_user
                ON schedules (created_by_user_id)
            """)
            # --- 人材管理テーブル ---
            cur.execute("""
                CREATE TABLE IF NOT EXISTS candidates (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    kana TEXT,
                    phone TEXT,
                    email TEXT,
                    applied_position TEXT,
                    status TEXT DEFAULT 'applied',
                    source TEXT,
                    years_of_experience INT,
                    expected_salary INT,
                    qualifications_text TEXT,
                    notes TEXT,
                    interview_date DATE,
                    discord_user_id TEXT,
                    created_by_user_id TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS employees (
                    id SERIAL PRIMARY KEY,
                    candidate_id INT REFERENCES candidates(id),
                    name TEXT NOT NULL,
                    kana TEXT,
                    email TEXT,
                    phone TEXT,
                    discord_user_id TEXT UNIQUE,
                    position TEXT NOT NULL,
                    employment_type TEXT NOT NULL,
                    hours_per_week NUMERIC(5,2),
                    monthly_salary INT,
                    hourly_wage INT,
                    hire_date DATE,
                    resignation_date DATE,
                    is_active BOOLEAN DEFAULT TRUE,
                    years_in_welfare NUMERIC(4,1),
                    service_assignment TEXT,
                    notes TEXT,
                    created_by_user_id TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS qualifications (
                    id SERIAL PRIMARY KEY,
                    code TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    category TEXT,
                    affects_revenue BOOLEAN DEFAULT TRUE,
                    description TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS employee_qualifications (
                    id SERIAL PRIMARY KEY,
                    employee_id INT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
                    qualification_id INT NOT NULL REFERENCES qualifications(id),
                    acquired_date DATE,
                    certificate_number TEXT,
                    notes TEXT,
                    UNIQUE(employee_id, qualification_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS addition_items (
                    id SERIAL PRIMARY KEY,
                    code TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    unit_value INT NOT NULL,
                    unit_type TEXT NOT NULL,
                    service_type TEXT NOT NULL,
                    category TEXT,
                    is_deduction BOOLEAN DEFAULT FALSE,
                    requirements_json JSONB,
                    auto_checkable BOOLEAN DEFAULT FALSE,
                    description TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS active_additions (
                    id SERIAL PRIMARY KEY,
                    addition_item_id INT NOT NULL REFERENCES addition_items(id),
                    service_type TEXT NOT NULL,
                    start_date DATE,
                    end_date DATE,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_candidates_status ON candidates (status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_employees_active ON employees (is_active, position)")
            # --- 資料管理テーブル ---
            cur.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT,
                    category TEXT,
                    tags TEXT[],
                    drive_file_id TEXT UNIQUE,
                    drive_web_link TEXT,
                    file_name TEXT,
                    mime_type TEXT,
                    file_size BIGINT,
                    text_content TEXT,
                    uploaded_by_user_id TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_category ON documents (category)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_tags ON documents USING GIN (tags)")
            # text_contentカラムが既存テーブルにない場合に追加
            cur.execute("""
                DO $$ BEGIN
                    ALTER TABLE documents ADD COLUMN text_content TEXT;
                EXCEPTION WHEN duplicate_column THEN NULL;
                END $$
            """)
        conn.commit()
    seed_qualifications()
    seed_addition_items()
    print("[DB] テーブル初期化完了")


# --- 会話履歴 ---

def save_message(user_id, channel_id, role, content):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO conversation_history (user_id, channel_id, role, content) VALUES (%s, %s, %s, %s)",
                (user_id, channel_id, role, content)
            )
        conn.commit()


def get_history(user_id, limit=MAX_HISTORY):
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT role, content FROM conversation_history
                   WHERE user_id = %s
                   ORDER BY created_at DESC LIMIT %s""",
                (user_id, limit)
            )
            rows = cur.fetchall()
    return list(reversed(rows))


def clear_history(user_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM conversation_history WHERE user_id = %s", (user_id,))
        conn.commit()


# --- タスク管理 ---

def get_tasks(user_id):
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT id, title, description, status, priority, due_date,
                          assignee_user_id, is_team_task, category, created_at
                   FROM tasks
                   WHERE (created_by_user_id = %s OR assignee_user_id = %s OR is_team_task = TRUE)
                     AND status != 'done'
                   ORDER BY
                     CASE priority
                       WHEN 'urgent' THEN 1 WHEN 'high' THEN 2
                       WHEN 'medium' THEN 3 WHEN 'low' THEN 4 ELSE 5
                     END,
                     due_date ASC NULLS LAST,
                     created_at DESC""",
                (user_id, user_id)
            )
            rows = cur.fetchall()
    for row in rows:
        if row.get("due_date") and isinstance(row["due_date"], date):
            row["due_date"] = row["due_date"].isoformat()
        if row.get("created_at"):
            row["created_at"] = str(row["created_at"])
    return rows


def create_task(user_id, title, description=None, priority="medium",
                due_date=None, assignee_user_id=None, is_team_task=False, category=None):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO tasks (title, description, priority, due_date,
                   assignee_user_id, created_by_user_id, is_team_task, category)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (title, description, priority, due_date,
                 assignee_user_id or user_id, user_id, is_team_task, category)
            )
            task_id = cur.fetchone()[0]
        conn.commit()
    return task_id


def update_task(task_id, **kwargs):
    allowed = {"title", "description", "status", "priority", "due_date",
               "assignee_user_id", "is_team_task", "category"}
    updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not updates:
        return False
    set_clause = ", ".join(f"{k} = %s" for k in updates)
    values = list(updates.values()) + [task_id]
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE tasks SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                values
            )
            affected = cur.rowcount
        conn.commit()
    return affected > 0


def delete_task(task_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM tasks WHERE id = %s", (task_id,))
            affected = cur.rowcount
        conn.commit()
    return affected > 0


# --- スケジュール管理 ---

def get_schedules(user_id):
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT id, title, description, schedule_type, start_date, start_time,
                          end_date, end_time, location, attendees, is_team_event,
                          category, created_at
                   FROM schedules
                   WHERE (created_by_user_id = %s OR is_team_event = TRUE)
                     AND start_date >= CURRENT_DATE
                   ORDER BY start_date ASC, start_time ASC NULLS FIRST""",
                (user_id,)
            )
            rows = cur.fetchall()
    for row in rows:
        for key in ("start_date", "end_date"):
            if row.get(key) and isinstance(row[key], date):
                row[key] = row[key].isoformat()
        for key in ("start_time", "end_time"):
            if row.get(key) and isinstance(row[key], time):
                row[key] = row[key].strftime("%H:%M")
        if row.get("created_at"):
            row["created_at"] = str(row["created_at"])
    return rows


def create_schedule(user_id, title, start_date, description=None, schedule_type="event",
                    start_time=None, end_date=None, end_time=None, location=None,
                    attendees=None, is_team_event=False, category=None):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO schedules (title, description, schedule_type, start_date,
                   start_time, end_date, end_time, location, attendees,
                   is_team_event, category, created_by_user_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (title, description, schedule_type, start_date,
                 start_time, end_date, end_time, location, attendees,
                 is_team_event, category, user_id)
            )
            schedule_id = cur.fetchone()[0]
        conn.commit()
    return schedule_id


def update_schedule(schedule_id, **kwargs):
    allowed = {"title", "description", "schedule_type", "start_date", "start_time",
               "end_date", "end_time", "location", "attendees", "is_team_event", "category"}
    updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not updates:
        return False
    set_clause = ", ".join(f"{k} = %s" for k in updates)
    values = list(updates.values()) + [schedule_id]
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE schedules SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                values
            )
            affected = cur.rowcount
        conn.commit()
    return affected > 0


def delete_schedule(schedule_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schedules WHERE id = %s", (schedule_id,))
            affected = cur.rowcount
        conn.commit()
    return affected > 0


# --- 資格マスタ・加算マスタの事前投入 ---

QUALIFICATIONS_SEED = [
    ("nurse", "看護師", "国家資格", "医療連携・看護職員配置加算の要件"),
    ("preschool_teacher", "保育士", "国家資格", "専門的支援加算・福祉専門職員配置加算の対象"),
    ("child_instructor", "児童指導員任用資格", "任用資格", "配置基準の基本職員"),
    ("jihatsu_kan", "児童発達支援管理責任者", "任用資格", "管理責任者配置の要件"),
    ("pt", "理学療法士", "国家資格", "専門的支援加算の対象"),
    ("ot", "作業療法士", "国家資格", "専門的支援加算の対象"),
    ("st", "言語聴覚士", "国家資格", "専門的支援加算の対象"),
    ("psychologist", "公認心理師", "国家資格", "専門的支援加算の対象"),
    ("clinical_psych", "臨床心理士", "任意資格", "専門的支援加算の対象"),
    ("social_worker", "社会福祉士", "国家資格", "福祉専門職員配置加算の対象"),
    ("care_worker", "介護福祉士", "国家資格", "福祉専門職員配置加算の対象"),
    ("teacher", "教員免許", "国家資格", "児童指導員任用の要件"),
    ("abuse_prevention", "虐待防止研修修了", "研修修了", "法定義務"),
    ("behavior_support", "強度行動障害支援者養成研修", "研修修了", "強度行動障害児支援加算の要件"),
    ("jihatsu_kan_kenshuu", "児発管研修修了", "研修修了", "児発管配置の要件"),
]


def seed_qualifications():
    with get_db() as conn:
        with conn.cursor() as cur:
            for code, name, category, description in QUALIFICATIONS_SEED:
                cur.execute(
                    """INSERT INTO qualifications (code, name, category, description)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (code) DO NOTHING""",
                    (code, name, category, description)
                )
        conn.commit()


# 加算マスタ（2024年報酬改定に基づく主要項目。単位数は概算値、要件は構造化）
# 正確な単位数は厚労省告示で要確認
ADDITION_ITEMS_SEED = [
    # 人員・体制系
    {"code": "fukushi_senmon_I", "name": "福祉専門職員配置等加算I", "unit_value": 15, "unit_type": "per_day",
     "service_type": "both", "category": "staff", "auto_checkable": True,
     "requirements_json": {"type": "qualified_ratio", "min_ratio": 0.35,
                           "qualifications": ["preschool_teacher", "social_worker", "care_worker", "nurse", "pt", "ot", "st", "psychologist"]},
     "description": "常勤指導員に占める国家資格保有者の割合が35%以上"},
    {"code": "fukushi_senmon_II", "name": "福祉専門職員配置等加算II", "unit_value": 10, "unit_type": "per_day",
     "service_type": "both", "category": "staff", "auto_checkable": True,
     "requirements_json": {"type": "qualified_ratio", "min_ratio": 0.25,
                           "qualifications": ["preschool_teacher", "social_worker", "care_worker", "nurse", "pt", "ot", "st", "psychologist"]},
     "description": "常勤指導員に占める国家資格保有者の割合が25%以上"},
    {"code": "fukushi_senmon_III", "name": "福祉専門職員配置等加算III", "unit_value": 6, "unit_type": "per_day",
     "service_type": "both", "category": "staff", "auto_checkable": True,
     "requirements_json": {"type": "full_time_ratio", "min_ratio": 0.75},
     "description": "常勤職員の割合が75%以上、または勤続3年以上の職員が30%以上"},
    {"code": "jidou_shidou_kahai_I", "name": "児童指導員等加配加算I", "unit_value": 187, "unit_type": "per_day",
     "service_type": "both", "category": "staff", "auto_checkable": True,
     "requirements_json": {"type": "extra_staff", "min_extra": 1, "must_be_qualified": True,
                           "qualifications": ["child_instructor", "preschool_teacher"]},
     "description": "基準人員+1名以上の有資格指導員配置"},
    {"code": "jidou_shidou_kahai_II", "name": "児童指導員等加配加算II", "unit_value": 123, "unit_type": "per_day",
     "service_type": "both", "category": "staff", "auto_checkable": True,
     "requirements_json": {"type": "extra_staff", "min_extra": 1, "must_be_qualified": False},
     "description": "基準人員+1名以上のその他の従業者配置"},
    {"code": "senmon_shien", "name": "専門的支援加算", "unit_value": 123, "unit_type": "per_day",
     "service_type": "both", "category": "staff", "auto_checkable": True,
     "requirements_json": {"type": "has_specialist",
                           "qualifications": ["pt", "ot", "st", "psychologist", "clinical_psych", "preschool_teacher", "child_instructor"]},
     "description": "PT/OT/ST/心理/保育士/児童指導員を常勤で1名以上"},
    {"code": "senmon_shien_jisshi", "name": "専門的支援実施加算", "unit_value": 150, "unit_type": "per_use",
     "service_type": "both", "category": "staff", "auto_checkable": False,
     "requirements_json": {"note": "月54回まで、計画的な個別・小集団支援"},
     "description": "専門的支援実施時（実施記録要）"},
    {"code": "nursing_placement", "name": "看護職員配置加算", "unit_value": 200, "unit_type": "per_day",
     "service_type": "both", "category": "staff", "auto_checkable": True,
     "requirements_json": {"type": "has_qualification", "qualification": "nurse", "employment_type": "full_time"},
     "description": "看護師を常勤で1名以上配置"},

    # 支援強化系
    {"code": "kyoudo_koudou", "name": "強度行動障害児支援加算", "unit_value": 200, "unit_type": "per_day",
     "service_type": "both", "category": "support", "auto_checkable": True,
     "requirements_json": {"type": "has_training", "qualification": "behavior_support"},
     "description": "強度行動障害支援者養成研修修了者の配置"},
    {"code": "tokubetsu_shien", "name": "特別支援加算", "unit_value": 54, "unit_type": "per_day",
     "service_type": "both", "category": "support", "auto_checkable": False,
     "description": "特別支援計画に基づく個別支援"},
    {"code": "kobetsu_support_I", "name": "個別サポート加算I", "unit_value": 100, "unit_type": "per_day",
     "service_type": "both", "category": "support", "auto_checkable": False,
     "description": "ケアニーズの高い児童への個別支援（I）"},
    {"code": "kobetsu_support_II", "name": "個別サポート加算II", "unit_value": 125, "unit_type": "per_day",
     "service_type": "both", "category": "support", "auto_checkable": False,
     "description": "虐待等の要保護児童への個別支援"},
    {"code": "nyuyoku_shien", "name": "入浴支援加算", "unit_value": 55, "unit_type": "per_day",
     "service_type": "both", "category": "support", "auto_checkable": False,
     "description": "入浴支援実施（2024新設）"},
    {"code": "shuuchuu_shien", "name": "集中的支援加算", "unit_value": 1000, "unit_type": "per_month",
     "service_type": "both", "category": "support", "auto_checkable": False,
     "description": "集中的支援実施（2024新設）"},
    {"code": "tsuusho_jiritsu", "name": "通所自立支援加算", "unit_value": 60, "unit_type": "per_day",
     "service_type": "houday", "category": "support", "auto_checkable": False,
     "description": "自立的な通所のための支援"},

    # 連携系
    {"code": "iryou_renkei_I", "name": "医療連携体制加算I", "unit_value": 500, "unit_type": "per_use",
     "service_type": "both", "category": "medical", "auto_checkable": False,
     "description": "看護職員の訪問による医療的ケア（1人）"},
    {"code": "iryou_renkei_II", "name": "医療連携体制加算II", "unit_value": 250, "unit_type": "per_use",
     "service_type": "both", "category": "medical", "auto_checkable": False,
     "description": "看護職員の訪問による医療的ケア（2人以上）"},
    {"code": "kateirenkei", "name": "家庭連携加算", "unit_value": 280, "unit_type": "per_use",
     "service_type": "both", "category": "linkage", "auto_checkable": False,
     "description": "保護者への相談援助（月4回まで）"},
    {"code": "kankei_renkei_I", "name": "関係機関連携加算I", "unit_value": 250, "unit_type": "per_use",
     "service_type": "both", "category": "linkage", "auto_checkable": False,
     "description": "保育所・学校等との連携"},
    {"code": "kankei_renkei_II", "name": "関係機関連携加算II", "unit_value": 200, "unit_type": "per_use",
     "service_type": "both", "category": "linkage", "auto_checkable": False,
     "description": "児童相談所等との連携"},
    {"code": "ikou_shien", "name": "保育・教育等移行支援加算", "unit_value": 500, "unit_type": "per_use",
     "service_type": "jihatsu", "category": "linkage", "auto_checkable": False,
     "description": "児発から保育所等への移行支援"},
    {"code": "houmon_shien", "name": "訪問支援特別加算", "unit_value": 187, "unit_type": "per_use",
     "service_type": "both", "category": "linkage", "auto_checkable": False,
     "description": "長期欠席児への家庭訪問支援"},

    # 運営系
    {"code": "encho_shien_1h", "name": "延長支援加算（1時間未満）", "unit_value": 61, "unit_type": "per_day",
     "service_type": "both", "category": "operation", "auto_checkable": False,
     "description": "計画時間を超えた支援（1時間未満）"},
    {"code": "encho_shien_2h", "name": "延長支援加算（1〜2時間）", "unit_value": 92, "unit_type": "per_day",
     "service_type": "both", "category": "operation", "auto_checkable": False,
     "description": "計画時間を超えた支援（1〜2時間）"},
    {"code": "souguu", "name": "送迎加算", "unit_value": 54, "unit_type": "per_day",
     "service_type": "both", "category": "operation", "auto_checkable": False,
     "description": "片道1回につき54単位"},
    {"code": "shokuji", "name": "食事提供加算", "unit_value": 30, "unit_type": "per_day",
     "service_type": "both", "category": "operation", "auto_checkable": False,
     "description": "食事提供体制"},
    {"code": "kesseki_taiou", "name": "欠席時対応加算", "unit_value": 94, "unit_type": "per_use",
     "service_type": "both", "category": "operation", "auto_checkable": False,
     "description": "急な欠席時の電話相談対応（月4回まで）"},

    # 処遇改善（％計算）
    {"code": "shogu_kaizen_I", "name": "福祉・介護職員等処遇改善加算I", "unit_value": 131, "unit_type": "percentage",
     "service_type": "both", "category": "treatment_improvement", "auto_checkable": False,
     "description": "ベース報酬の13.1%（キャリアパス要件等）"},

    # 減算項目
    {"code": "shogu_kaizen_deduction", "name": "自己評価結果等未公表減算", "unit_value": -15, "unit_type": "percentage",
     "service_type": "both", "category": "deduction", "is_deduction": True, "auto_checkable": False,
     "description": "基本報酬の15%減算"},
    {"code": "shintai_kousoku_deduction", "name": "身体拘束廃止未実施減算", "unit_value": -5, "unit_type": "per_day",
     "service_type": "both", "category": "deduction", "is_deduction": True, "auto_checkable": False,
     "description": "1日5単位減算"},
]


def seed_addition_items():
    with get_db() as conn:
        with conn.cursor() as cur:
            for item in ADDITION_ITEMS_SEED:
                cur.execute(
                    """INSERT INTO addition_items
                       (code, name, unit_value, unit_type, service_type, category,
                        is_deduction, requirements_json, auto_checkable, description)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (code) DO UPDATE SET
                         name = EXCLUDED.name,
                         unit_value = EXCLUDED.unit_value,
                         requirements_json = EXCLUDED.requirements_json,
                         description = EXCLUDED.description""",
                    (item["code"], item["name"], item["unit_value"], item["unit_type"],
                     item["service_type"], item["category"],
                     item.get("is_deduction", False),
                     json.dumps(item.get("requirements_json", {}), ensure_ascii=False),
                     item.get("auto_checkable", False),
                     item.get("description", ""))
                )
        conn.commit()


def get_qualifications():
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, code, name, category, description FROM qualifications ORDER BY id")
            return cur.fetchall()


def get_addition_items(service_type=None):
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if service_type and service_type != "both":
                cur.execute(
                    """SELECT id, code, name, unit_value, unit_type, service_type, category,
                              is_deduction, requirements_json, auto_checkable, description
                       FROM addition_items
                       WHERE service_type = %s OR service_type = 'both'
                       ORDER BY category, id""",
                    (service_type,)
                )
            else:
                cur.execute(
                    """SELECT id, code, name, unit_value, unit_type, service_type, category,
                              is_deduction, requirements_json, auto_checkable, description
                       FROM addition_items ORDER BY category, id"""
                )
            return cur.fetchall()


# --- 候補者管理 ---

def get_candidates():
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT id, name, kana, phone, email, applied_position, status, source,
                          years_of_experience, expected_salary, qualifications_text,
                          notes, interview_date, created_at
                   FROM candidates
                   WHERE status NOT IN ('hired', 'rejected', 'declined')
                   ORDER BY created_at DESC"""
            )
            rows = cur.fetchall()
    for row in rows:
        if row.get("interview_date") and isinstance(row["interview_date"], date):
            row["interview_date"] = row["interview_date"].isoformat()
        if row.get("created_at"):
            row["created_at"] = str(row["created_at"])
    return rows


def create_candidate(user_id, name, **kwargs):
    allowed = {"kana", "phone", "email", "applied_position", "status", "source",
               "years_of_experience", "expected_salary", "qualifications_text",
               "notes", "interview_date"}
    fields = {"name": name, "created_by_user_id": user_id}
    fields.update({k: v for k, v in kwargs.items() if k in allowed and v is not None})
    cols = ", ".join(fields.keys())
    placeholders = ", ".join(["%s"] * len(fields))
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO candidates ({cols}) VALUES ({placeholders}) RETURNING id",
                list(fields.values())
            )
            candidate_id = cur.fetchone()[0]
        conn.commit()
    return candidate_id


def update_candidate(candidate_id, **kwargs):
    allowed = {"name", "kana", "phone", "email", "applied_position", "status", "source",
               "years_of_experience", "expected_salary", "qualifications_text",
               "notes", "interview_date"}
    updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not updates:
        return False
    set_clause = ", ".join(f"{k} = %s" for k in updates)
    values = list(updates.values()) + [candidate_id]
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE candidates SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                values
            )
            affected = cur.rowcount
        conn.commit()
    return affected > 0


def delete_candidate(candidate_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM candidates WHERE id = %s", (candidate_id,))
            affected = cur.rowcount
        conn.commit()
    return affected > 0


# --- 従業員管理 ---

def get_employees(active_only=True):
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            where = "WHERE is_active = TRUE" if active_only else ""
            cur.execute(
                f"""SELECT e.id, e.name, e.kana, e.email, e.phone, e.discord_user_id,
                          e.position, e.employment_type, e.hours_per_week,
                          e.monthly_salary, e.hourly_wage, e.hire_date,
                          e.is_active, e.years_in_welfare, e.service_assignment,
                          e.notes, e.created_at,
                          COALESCE(
                              (SELECT json_agg(q.code)
                               FROM employee_qualifications eq
                               JOIN qualifications q ON eq.qualification_id = q.id
                               WHERE eq.employee_id = e.id),
                              '[]'::json
                          ) AS qualification_codes
                   FROM employees e {where}
                   ORDER BY e.hire_date DESC NULLS LAST, e.id"""
            )
            rows = cur.fetchall()
    for row in rows:
        if row.get("hire_date") and isinstance(row["hire_date"], date):
            row["hire_date"] = row["hire_date"].isoformat()
        if row.get("created_at"):
            row["created_at"] = str(row["created_at"])
    return rows


def create_employee(user_id, name, position, employment_type, **kwargs):
    allowed = {"candidate_id", "kana", "email", "phone", "discord_user_id",
               "hours_per_week", "monthly_salary", "hourly_wage", "hire_date",
               "years_in_welfare", "service_assignment", "notes"}
    fields = {
        "name": name, "position": position, "employment_type": employment_type,
        "created_by_user_id": user_id
    }
    fields.update({k: v for k, v in kwargs.items() if k in allowed and v is not None})
    cols = ", ".join(fields.keys())
    placeholders = ", ".join(["%s"] * len(fields))
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO employees ({cols}) VALUES ({placeholders}) RETURNING id",
                list(fields.values())
            )
            employee_id = cur.fetchone()[0]
        conn.commit()
    return employee_id


def update_employee(employee_id, **kwargs):
    allowed = {"name", "kana", "email", "phone", "discord_user_id", "position",
               "employment_type", "hours_per_week", "monthly_salary", "hourly_wage",
               "hire_date", "resignation_date", "is_active", "years_in_welfare",
               "service_assignment", "notes"}
    updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not updates:
        return False
    set_clause = ", ".join(f"{k} = %s" for k in updates)
    values = list(updates.values()) + [employee_id]
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE employees SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                values
            )
            affected = cur.rowcount
        conn.commit()
    return affected > 0


def deactivate_employee(employee_id, resignation_date=None):
    return update_employee(
        employee_id,
        is_active=False,
        resignation_date=resignation_date or date.today().isoformat()
    )


def add_qualification(employee_id, qualification_code):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM qualifications WHERE code = %s", (qualification_code,))
            row = cur.fetchone()
            if not row:
                return False
            qualification_id = row[0]
            cur.execute(
                """INSERT INTO employee_qualifications (employee_id, qualification_id)
                   VALUES (%s, %s)
                   ON CONFLICT (employee_id, qualification_id) DO NOTHING""",
                (employee_id, qualification_id)
            )
        conn.commit()
    return True


def remove_qualification(employee_id, qualification_code):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """DELETE FROM employee_qualifications
                   WHERE employee_id = %s
                     AND qualification_id = (SELECT id FROM qualifications WHERE code = %s)""",
                (employee_id, qualification_code)
            )
            affected = cur.rowcount
        conn.commit()
    return affected > 0


def promote_candidate_to_employee(candidate_id, position, employment_type, user_id,
                                  monthly_salary=None, hire_date=None, hours_per_week=None):
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM candidates WHERE id = %s", (candidate_id,))
            cand = cur.fetchone()
            if not cand:
                return None
            cur.execute(
                """INSERT INTO employees (candidate_id, name, kana, email, phone,
                   position, employment_type, monthly_salary, hire_date, hours_per_week,
                   years_in_welfare, created_by_user_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (candidate_id, cand["name"], cand.get("kana"), cand.get("email"),
                 cand.get("phone"), position, employment_type, monthly_salary,
                 hire_date, hours_per_week, cand.get("years_of_experience"), user_id)
            )
            employee_id = cur.fetchone()[0]
            cur.execute(
                "UPDATE candidates SET status = 'hired', updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                (candidate_id,)
            )
        conn.commit()
    return employee_id


# --- 報酬算定ロジック ---

UNIT_PRICE = 10.70  # 5級地
CAPACITY_JIHATSU = 7
CAPACITY_HOUDAY = 9

# 基本報酬単位数（2024改定、定員10名以下、主として重症心身障害児以外）
# 正確な単位数は厚労省告示で要確認
BASE_UNITS = {
    "jihatsu": {
        "default": 901,  # 1.5時間以上
        "short": 576,    # 1.5時間未満
    },
    "houday": {
        "weekday_short": 274,    # 〜30分
        "weekday_mid": 574,      # 1〜2時間
        "weekday_long": 609,     # 3時間〜
        "holiday_short": 295,
        "holiday_long": 721,
        "default": 609,
    }
}


def calculate_base_revenue(service_type, attendance_days_per_month, time_band="default"):
    """基本報酬を計算（月次）"""
    units = BASE_UNITS.get(service_type, {}).get(time_band, 0)
    total_units = units * attendance_days_per_month
    return {
        "units_per_day": units,
        "attendance_days": attendance_days_per_month,
        "total_units": total_units,
        "amount": int(total_units * UNIT_PRICE)
    }


def check_addition_eligibility(addition, employees, service_type):
    """加算の要件を充足するかチェック。auto_checkable=Trueのもののみ判定。"""
    req = addition.get("requirements_json") or {}
    if isinstance(req, str):
        try:
            req = json.loads(req)
        except (json.JSONDecodeError, TypeError):
            req = {}

    if not addition.get("auto_checkable"):
        return None  # 手動判定

    # サービス種別フィルタ
    active = [
        e for e in employees
        if e.get("is_active")
        and (not e.get("service_assignment") or e["service_assignment"] in (service_type, "both"))
    ]

    req_type = req.get("type")

    if req_type == "has_qualification":
        target_qual = req.get("qualification")
        emp_type = req.get("employment_type")
        for e in active:
            quals = e.get("qualification_codes") or []
            if target_qual in quals:
                if not emp_type or e.get("employment_type") == emp_type:
                    return True
        return False

    if req_type == "has_specialist":
        target_quals = set(req.get("qualifications", []))
        for e in active:
            quals = set(e.get("qualification_codes") or [])
            if quals & target_quals and e.get("employment_type") == "full_time":
                return True
        return False

    if req_type == "has_training":
        target_qual = req.get("qualification")
        for e in active:
            quals = e.get("qualification_codes") or []
            if target_qual in quals:
                return True
        return False

    if req_type == "qualified_ratio":
        instructors = [e for e in active if e.get("employment_type") == "full_time"
                       and e.get("position") in ("child_instructor", "指導員", "hoikushi", "保育士")]
        if not instructors:
            return False
        target_quals = set(req.get("qualifications", []))
        qualified = sum(1 for e in instructors
                        if set(e.get("qualification_codes") or []) & target_quals)
        ratio = qualified / len(instructors) if instructors else 0
        return ratio >= req.get("min_ratio", 0)

    if req_type == "full_time_ratio":
        total = len(active)
        if not total:
            return False
        full_time = sum(1 for e in active if e.get("employment_type") == "full_time")
        return (full_time / total) >= req.get("min_ratio", 0)

    if req_type == "extra_staff":
        # 基準人員を超える指導員/保育士の配置
        base_required = 2  # 定員10名以下は指導員2名以上が基準
        must_be_qualified = req.get("must_be_qualified", False)
        target_quals = set(req.get("qualifications", []))
        staff = [e for e in active
                 if e.get("position") in ("child_instructor", "hoikushi", "指導員", "保育士")]
        if must_be_qualified:
            staff = [e for e in staff if set(e.get("qualification_codes") or []) & target_quals]
        return len(staff) >= base_required + req.get("min_extra", 1)

    return None


def calculate_monthly_revenue(service_type, attendance_days_per_month, employees=None,
                              time_band="default"):
    """指定サービスの月次報酬を計算"""
    if employees is None:
        employees = get_employees(active_only=True)

    base = calculate_base_revenue(service_type, attendance_days_per_month, time_band)
    additions = get_addition_items(service_type)

    eligible_additions = []
    pending_additions = []  # 手動判定が必要
    total_addition_units = 0

    for add in additions:
        if add.get("is_deduction"):
            continue
        if add.get("unit_type") == "percentage":
            continue  # 処遇改善は後で計算
        eligible = check_addition_eligibility(add, employees, service_type)
        if eligible is True:
            units = add["unit_value"]
            if add["unit_type"] == "per_day":
                month_units = units * attendance_days_per_month
            else:
                month_units = units  # 概算（per_use系は手動判定相当）
            total_addition_units += month_units
            eligible_additions.append({
                "code": add["code"], "name": add["name"],
                "units": units, "unit_type": add["unit_type"],
                "monthly_units": month_units
            })
        elif eligible is None and not add.get("is_deduction"):
            pending_additions.append({"code": add["code"], "name": add["name"]})

    subtotal_units = base["total_units"] + total_addition_units
    subtotal_amount = int(subtotal_units * UNIT_PRICE)

    # 処遇改善加算I（13.1%）を適用
    treatment_rate = 0.131
    treatment_amount = int(subtotal_amount * treatment_rate)
    total_amount = subtotal_amount + treatment_amount

    return {
        "service_type": service_type,
        "base": base,
        "eligible_additions": eligible_additions,
        "pending_additions": pending_additions,
        "addition_units": total_addition_units,
        "subtotal_amount": subtotal_amount,
        "treatment_improvement": treatment_amount,
        "total_amount": total_amount
    }


def simulate_hire_impact(new_employee_spec, service_type="both",
                         jihatsu_days=140, houday_days=180):
    """採用による月次収益への影響をシミュレーション"""
    current_employees = get_employees(active_only=True)
    # 仮想従業員を追加
    virtual = {
        "id": -1,
        "name": new_employee_spec.get("name", "仮想採用者"),
        "position": new_employee_spec.get("position", "child_instructor"),
        "employment_type": new_employee_spec.get("employment_type", "full_time"),
        "service_assignment": new_employee_spec.get("service_assignment", "both"),
        "qualification_codes": new_employee_spec.get("qualifications", []),
        "is_active": True,
        "hours_per_week": new_employee_spec.get("hours_per_week"),
    }
    simulated_employees = current_employees + [virtual]

    def calc_total(emps):
        if service_type == "jihatsu":
            return calculate_monthly_revenue("jihatsu", jihatsu_days, emps)["total_amount"]
        if service_type == "houday":
            return calculate_monthly_revenue("houday", houday_days, emps)["total_amount"]
        j = calculate_monthly_revenue("jihatsu", jihatsu_days, emps)["total_amount"]
        h = calculate_monthly_revenue("houday", houday_days, emps)["total_amount"]
        return j + h

    current_total = calc_total(current_employees)
    simulated_total = calc_total(simulated_employees)

    return {
        "current_monthly": current_total,
        "simulated_monthly": simulated_total,
        "diff_monthly": simulated_total - current_total,
        "diff_annual": (simulated_total - current_total) * 12,
        "new_employee": virtual
    }


# --- リマインダー ---

def get_upcoming_reminders():
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)
    week_end = today + timedelta(days=(6 - today.weekday()))
    results = {
        "overdue_tasks": [],
        "today_tasks": [],
        "tomorrow_tasks": [],
        "week_tasks": [],
        "today_schedules": [],
        "tomorrow_schedules": [],
        "week_schedules": [],
    }
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 期限切れタスク
            cur.execute(
                """SELECT id, title, priority, due_date FROM tasks
                   WHERE due_date < %s AND status != 'done'
                   ORDER BY due_date ASC""",
                (today,)
            )
            results["overdue_tasks"] = cur.fetchall()
            # 今日期限のタスク
            cur.execute(
                """SELECT id, title, priority, due_date FROM tasks
                   WHERE due_date = %s AND status != 'done'""",
                (today,)
            )
            results["today_tasks"] = cur.fetchall()
            # 明日期限のタスク
            cur.execute(
                """SELECT id, title, priority, due_date FROM tasks
                   WHERE due_date = %s AND status != 'done'""",
                (tomorrow,)
            )
            results["tomorrow_tasks"] = cur.fetchall()
            # 今週残り（明後日〜週末）
            if week_end > tomorrow:
                cur.execute(
                    """SELECT id, title, priority, due_date FROM tasks
                       WHERE due_date > %s AND due_date <= %s AND status != 'done'
                       ORDER BY due_date ASC""",
                    (tomorrow, week_end)
                )
                results["week_tasks"] = cur.fetchall()
            # 今日の予定
            cur.execute(
                """SELECT id, title, schedule_type, start_date, start_time, location
                   FROM schedules WHERE start_date = %s""",
                (today,)
            )
            results["today_schedules"] = cur.fetchall()
            # 明日の予定
            cur.execute(
                """SELECT id, title, schedule_type, start_date, start_time, location
                   FROM schedules WHERE start_date = %s AND reminder_sent = FALSE""",
                (tomorrow,)
            )
            results["tomorrow_schedules"] = cur.fetchall()
            # 今週残りの予定
            if week_end > tomorrow:
                cur.execute(
                    """SELECT id, title, schedule_type, start_date, start_time, location
                       FROM schedules WHERE start_date > %s AND start_date <= %s
                       ORDER BY start_date ASC""",
                    (tomorrow, week_end)
                )
                results["week_schedules"] = cur.fetchall()
            # リマインダー送信済みフラグ
            cur.execute(
                "UPDATE schedules SET reminder_sent = TRUE WHERE start_date = %s",
                (tomorrow,)
            )
        conn.commit()
    return results


# --- 操作の実行 ---

def execute_task_ops(user_id, task_ops):
    results = []
    for op in task_ops:
        action = op.get("action")
        try:
            if action == "create":
                task_id = create_task(
                    user_id=user_id,
                    title=op.get("title", ""),
                    description=op.get("description"),
                    priority=op.get("priority", "medium"),
                    due_date=op.get("due_date"),
                    assignee_user_id=op.get("assignee_user_id"),
                    is_team_task=op.get("is_team_task", False),
                    category=op.get("category")
                )
                results.append(f"[Task#{task_id}] 作成完了")
            elif action == "update":
                tid = op.get("task_id")
                fields = {k: v for k, v in op.items() if k not in ("action", "task_id")}
                if update_task(tid, **fields):
                    results.append(f"[Task#{tid}] 更新完了")
            elif action == "complete":
                tid = op.get("task_id")
                if update_task(tid, status="done"):
                    results.append(f"[Task#{tid}] 完了")
            elif action == "delete":
                tid = op.get("task_id")
                if delete_task(tid):
                    results.append(f"[Task#{tid}] 削除完了")
        except Exception as e:
            print(f"[TaskOp Error] {action}: {e}")
            results.append(f"[TaskOp Error] {action}: {str(e)[:50]}")
    return results


def execute_schedule_ops(user_id, schedule_ops):
    results = []
    for op in schedule_ops:
        action = op.get("action")
        try:
            if action == "create":
                sid = create_schedule(
                    user_id=user_id,
                    title=op.get("title", ""),
                    start_date=op.get("start_date"),
                    description=op.get("description"),
                    schedule_type=op.get("schedule_type", "event"),
                    start_time=op.get("start_time"),
                    end_date=op.get("end_date"),
                    end_time=op.get("end_time"),
                    location=op.get("location"),
                    attendees=op.get("attendees"),
                    is_team_event=op.get("is_team_event", False),
                    category=op.get("category")
                )
                results.append(f"[Schedule#{sid}] 作成完了")
            elif action == "update":
                sid = op.get("schedule_id")
                fields = {k: v for k, v in op.items() if k not in ("action", "schedule_id")}
                if update_schedule(sid, **fields):
                    results.append(f"[Schedule#{sid}] 更新完了")
            elif action == "delete":
                sid = op.get("schedule_id")
                if delete_schedule(sid):
                    results.append(f"[Schedule#{sid}] 削除完了")
        except Exception as e:
            print(f"[ScheduleOp Error] {action}: {e}")
            results.append(f"[ScheduleOp Error] {action}: {str(e)[:50]}")
    return results


def get_documents(category=None, tags=None, search_text=None, limit=50,
                  include_content_preview=True, preview_chars=800):
    conditions = []
    params = []
    if category:
        conditions.append("category = %s")
        params.append(category)
    if tags:
        conditions.append("tags && %s")
        params.append(tags)
    if search_text:
        conditions.append(
            "(title ILIKE %s OR description ILIKE %s OR file_name ILIKE %s OR text_content ILIKE %s)"
        )
        wild = f"%{search_text}%"
        params.extend([wild, wild, wild, wild])
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    # 本文プレビュー列（指定文字数で切る）
    content_expr = f"LEFT(text_content, {int(preview_chars)})" if include_content_preview else "NULL"
    query = f"""SELECT id, title, description, category, tags, drive_file_id,
                       drive_web_link, file_name, mime_type, file_size,
                       {content_expr} AS text_preview,
                       (text_content IS NOT NULL) AS has_text,
                       LENGTH(text_content) AS text_length,
                       created_at
                FROM documents {where}
                ORDER BY created_at DESC LIMIT %s"""
    params.append(limit)
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    for row in rows:
        if row.get("created_at"):
            row["created_at"] = str(row["created_at"])
    return rows


def get_document_full_text(doc_id):
    """指定したドキュメントの本文全文を取得"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT text_content FROM documents WHERE id = %s", (doc_id,))
            row = cur.fetchone()
    return row[0] if row and row[0] else None


def create_document(user_id, title, drive_file_id=None, drive_web_link=None,
                    description=None, category=None, tags=None,
                    file_name=None, mime_type=None, file_size=None,
                    text_content=None):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO documents (title, description, category, tags,
                   drive_file_id, drive_web_link, file_name, mime_type, file_size,
                   text_content, uploaded_by_user_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (drive_file_id) DO UPDATE SET
                     title = EXCLUDED.title,
                     description = COALESCE(EXCLUDED.description, documents.description),
                     category = COALESCE(EXCLUDED.category, documents.category),
                     tags = COALESCE(EXCLUDED.tags, documents.tags),
                     text_content = COALESCE(EXCLUDED.text_content, documents.text_content),
                     updated_at = CURRENT_TIMESTAMP
                   RETURNING id""",
                (title, description, category, tags or [],
                 drive_file_id, drive_web_link, file_name, mime_type, file_size,
                 text_content, user_id)
            )
            doc_id = cur.fetchone()[0]
        conn.commit()
    return doc_id


def update_document(doc_id, **kwargs):
    allowed = {"title", "description", "category", "tags", "text_content", "file_name"}
    updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not updates:
        return False
    set_clause = ", ".join(f"{k} = %s" for k in updates)
    values = list(updates.values()) + [doc_id]
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE documents SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                values
            )
            affected = cur.rowcount
        conn.commit()
    return affected > 0


def delete_document(doc_id, delete_from_drive_too=True):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT drive_file_id FROM documents WHERE id = %s", (doc_id,))
            row = cur.fetchone()
            if not row:
                return False
            drive_file_id = row[0]
            cur.execute("DELETE FROM documents WHERE id = %s", (doc_id,))
        conn.commit()
    if delete_from_drive_too and drive_file_id:
        delete_from_drive(drive_file_id)
    return True


def scan_and_import_drive(user_id):
    """互換用エイリアス：sync_drive_to_db を呼ぶ"""
    return sync_drive_to_db(user_id)


def sync_drive_to_db(user_id="auto_sync"):
    """Drive→DB の一方向同期。Driveを正、DBをミラーとする。

    - Driveに新規追加されたファイル → DBに追加（テキストなら中身も取得）
    - Driveで更新されたファイル → DBの該当レコードを更新（ファイル名/サイズ比較）
    - Driveから削除されたファイル → DBからも削除（drive_file_id を持つもののみ対象）
    - Discord添付経由で登録された drive_file_id=NULL のレコードは触らない
    """
    drive_files = list_drive_folder_files()
    drive_id_set = {f["id"] for f in drive_files}

    # DB内のDrive由来ドキュメント一覧
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT id, drive_file_id, file_name, file_size
                   FROM documents
                   WHERE drive_file_id IS NOT NULL"""
            )
            existing = cur.fetchall()
    by_drive_id = {row["drive_file_id"]: row for row in existing}

    added = 0
    updated = 0
    removed = 0
    errors = 0

    # 追加・更新処理
    for f in drive_files:
        fid = f.get("id")
        name = f.get("name", "unknown")
        size = int(f.get("size", 0)) if f.get("size") else 0
        try:
            if fid not in by_drive_id:
                # 新規追加：テキストファイルなら中身も取得
                text_content = None
                if is_text_file_meta(f):
                    text_content = fetch_drive_text_content(fid)
                create_document(
                    user_id=user_id,
                    title=name,
                    drive_file_id=fid,
                    drive_web_link=f.get("webViewLink"),
                    file_name=name,
                    mime_type=f.get("mimeType"),
                    file_size=size,
                    text_content=text_content,
                )
                added += 1
                print(f"[Sync] +ADD: {name}")
            else:
                # 既存：ファイル名/サイズ変更を検知
                row = by_drive_id[fid]
                needs_update = (row["file_name"] != name) or (row["file_size"] != size)
                if needs_update:
                    text_content = None
                    if is_text_file_meta(f):
                        text_content = fetch_drive_text_content(fid)
                    update_document(
                        row["id"],
                        title=name,
                        file_name=name,
                        text_content=text_content,
                    )
                    with get_db() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE documents SET file_size = %s WHERE id = %s",
                                (size, row["id"]),
                            )
                        conn.commit()
                    updated += 1
                    print(f"[Sync] *UPD: {name}")
        except Exception as e:
            errors += 1
            print(f"[Sync Error] {name}: {e}")

    # 削除処理：Driveから消えたファイルをDBから削除
    for fid, row in by_drive_id.items():
        if fid not in drive_id_set:
            try:
                with get_db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM documents WHERE id = %s", (row["id"],))
                    conn.commit()
                removed += 1
                print(f"[Sync] -REM: {row['file_name']}")
            except Exception as e:
                errors += 1
                print(f"[Sync Error] remove {row['file_name']}: {e}")

    print(f"[Sync] 完了: +{added} *{updated} -{removed} (errors={errors})")
    return {"added": added, "updated": updated, "removed": removed,
            "errors": errors, "total": len(drive_files),
            # 旧API互換
            "imported": added, "skipped": len(drive_files) - added - updated}


# --- 資料ops実行 ---

def execute_doc_ops(user_id, doc_ops):
    results = []
    for op in doc_ops:
        action = op.get("action")
        try:
            if action == "register":
                doc_id = create_document(
                    user_id=user_id,
                    title=op.get("title", ""),
                    drive_file_id=op.get("drive_file_id"),
                    drive_web_link=op.get("drive_web_link"),
                    description=op.get("description"),
                    category=op.get("category"),
                    tags=op.get("tags"),
                    file_name=op.get("file_name"),
                    mime_type=op.get("mime_type"),
                    file_size=op.get("file_size")
                )
                results.append(f"[Doc#{doc_id}] 登録完了")
            elif action == "update":
                did = op.get("doc_id")
                fields = {k: v for k, v in op.items() if k not in ("action", "doc_id")}
                if update_document(did, **fields):
                    # DriveファイルID取得
                    with get_db() as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT drive_file_id, file_name FROM documents WHERE id = %s", (did,))
                            row = cur.fetchone()
                    drive_fid = row[0] if row else None
                    old_fname = row[1] if row else None

                    # タイトル変更時はDrive上のファイル名もリネーム
                    new_title = op.get("title")
                    if new_title and drive_fid:
                        import os as _os
                        ext = _os.path.splitext(old_fname or "")[1] if old_fname else ""
                        new_name = new_title if new_title.endswith(ext) else new_title + ext
                        rename_drive_file(drive_fid, new_name)
                        with get_db() as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE documents SET file_name = %s WHERE id = %s",
                                    (new_name, did)
                                )
                            conn.commit()

                    # カテゴリ変更時はDriveの正しいフォルダに移動
                    new_category = op.get("category")
                    if new_category and drive_fid:
                        target_folder = resolve_drive_folder(new_category)
                        move_drive_file_to_folder(drive_fid, target_folder)
                        print(f"[DocOps] Doc#{did} moved to folder: {new_category}")

                    results.append(f"[Doc#{did}] 更新完了")
            elif action == "delete":
                did = op.get("doc_id")
                if delete_document(did):
                    results.append(f"[Doc#{did}] 削除完了")
            elif action == "scan":
                r = scan_and_import_drive(user_id)
                results.append(f"[Scan] 取り込み {r['imported']}件 / スキップ {r['skipped']}件")
        except Exception as e:
            print(f"[DocOp Error] {action}: {e}")
            results.append(f"[DocOp Error] {action}: {str(e)[:80]}")
    return results


def execute_hr_ops(user_id, hr_ops):
    results = []
    for op in hr_ops:
        action = op.get("action")
        try:
            if action == "create_candidate":
                cid = create_candidate(
                    user_id=user_id,
                    name=op.get("name", ""),
                    kana=op.get("kana"),
                    phone=op.get("phone"),
                    email=op.get("email"),
                    applied_position=op.get("applied_position"),
                    status=op.get("status", "applied"),
                    source=op.get("source"),
                    years_of_experience=op.get("years_of_experience"),
                    expected_salary=op.get("expected_salary"),
                    qualifications_text=op.get("qualifications_text"),
                    notes=op.get("notes"),
                    interview_date=op.get("interview_date")
                )
                results.append(f"[Candidate#{cid}] 登録完了")
            elif action == "update_candidate":
                cid = op.get("candidate_id")
                fields = {k: v for k, v in op.items() if k not in ("action", "candidate_id")}
                if update_candidate(cid, **fields):
                    results.append(f"[Candidate#{cid}] 更新完了")
            elif action == "delete_candidate":
                cid = op.get("candidate_id")
                if delete_candidate(cid):
                    results.append(f"[Candidate#{cid}] 削除完了")
            elif action == "promote_to_employee":
                cid = op.get("candidate_id")
                eid = promote_candidate_to_employee(
                    candidate_id=cid,
                    position=op.get("position", "child_instructor"),
                    employment_type=op.get("employment_type", "full_time"),
                    user_id=user_id,
                    monthly_salary=op.get("monthly_salary"),
                    hire_date=op.get("hire_date"),
                    hours_per_week=op.get("hours_per_week")
                )
                if eid:
                    # 候補者にqualification_codesがあれば付与
                    for qcode in op.get("qualifications", []):
                        add_qualification(eid, qcode)
                    results.append(f"[Employee#{eid}] 社員登録完了（Candidate#{cid}から昇格）")
            elif action == "create_employee":
                eid = create_employee(
                    user_id=user_id,
                    name=op.get("name", ""),
                    position=op.get("position", "child_instructor"),
                    employment_type=op.get("employment_type", "full_time"),
                    kana=op.get("kana"),
                    email=op.get("email"),
                    phone=op.get("phone"),
                    discord_user_id=op.get("discord_user_id"),
                    hours_per_week=op.get("hours_per_week"),
                    monthly_salary=op.get("monthly_salary"),
                    hourly_wage=op.get("hourly_wage"),
                    hire_date=op.get("hire_date"),
                    years_in_welfare=op.get("years_in_welfare"),
                    service_assignment=op.get("service_assignment", "both"),
                    notes=op.get("notes")
                )
                for qcode in op.get("qualifications", []):
                    add_qualification(eid, qcode)
                results.append(f"[Employee#{eid}] 作成完了")
            elif action == "update_employee":
                eid = op.get("employee_id")
                fields = {k: v for k, v in op.items() if k not in ("action", "employee_id", "qualifications")}
                if update_employee(eid, **fields):
                    results.append(f"[Employee#{eid}] 更新完了")
                for qcode in op.get("qualifications", []):
                    add_qualification(eid, qcode)
            elif action == "deactivate_employee":
                eid = op.get("employee_id")
                if deactivate_employee(eid, resignation_date=op.get("resignation_date")):
                    results.append(f"[Employee#{eid}] 退職処理完了")
            elif action == "add_qualification":
                eid = op.get("employee_id")
                qcode = op.get("qualification_code")
                if add_qualification(eid, qcode):
                    results.append(f"[Employee#{eid}] 資格追加: {qcode}")
            elif action == "remove_qualification":
                eid = op.get("employee_id")
                qcode = op.get("qualification_code")
                if remove_qualification(eid, qcode):
                    results.append(f"[Employee#{eid}] 資格削除: {qcode}")
        except Exception as e:
            print(f"[HROp Error] {action}: {e}")
            results.append(f"[HROp Error] {action}: {str(e)[:80]}")
    return results


def execute_revenue_ops(user_id, revenue_ops):
    results = []
    for op in revenue_ops:
        action = op.get("action")
        try:
            if action == "calculate":
                service_type = op.get("service_type", "both")
                jihatsu_days = op.get("jihatsu_days", 140)
                houday_days = op.get("houday_days", 180)
                if service_type in ("jihatsu", "both"):
                    j = calculate_monthly_revenue("jihatsu", jihatsu_days)
                    results.append({"type": "jihatsu_revenue", "data": j})
                if service_type in ("houday", "both"):
                    h = calculate_monthly_revenue("houday", houday_days)
                    results.append({"type": "houday_revenue", "data": h})
            elif action == "simulate_hire":
                spec = {k: v for k, v in op.items() if k not in ("action", "service_type", "jihatsu_days", "houday_days")}
                sim = simulate_hire_impact(
                    spec,
                    service_type=op.get("service_type", "both"),
                    jihatsu_days=op.get("jihatsu_days", 140),
                    houday_days=op.get("houday_days", 180)
                )
                results.append({"type": "simulation", "data": sim})
        except Exception as e:
            print(f"[RevenueOp Error] {action}: {e}")
            results.append({"type": "error", "data": f"{action}: {str(e)[:80]}"})
    return results


def format_revenue_result(results):
    """報酬算定・シミュレーション結果を人間可読に整形"""
    lines = []
    for r in results:
        t = r.get("type")
        d = r.get("data", {})
        if t == "jihatsu_revenue":
            lines.append("**【児童発達支援】月次報酬見込み**")
            lines.append(f"- 基本報酬: {d['base']['total_units']}単位 × 10.70円 = {d['base']['amount']:,}円")
            if d["eligible_additions"]:
                lines.append(f"- 取得可能加算: {len(d['eligible_additions'])}項目（{d['addition_units']:,}単位）")
                for add in d["eligible_additions"][:5]:
                    lines.append(f"  - {add['name']}: {add['units']}単位")
            lines.append(f"- 処遇改善加算: {d['treatment_improvement']:,}円")
            lines.append(f"- **月額合計: {d['total_amount']:,}円**\n")
        elif t == "houday_revenue":
            lines.append("**【放課後等デイサービス】月次報酬見込み**")
            lines.append(f"- 基本報酬: {d['base']['total_units']}単位 × 10.70円 = {d['base']['amount']:,}円")
            if d["eligible_additions"]:
                lines.append(f"- 取得可能加算: {len(d['eligible_additions'])}項目（{d['addition_units']:,}単位）")
                for add in d["eligible_additions"][:5]:
                    lines.append(f"  - {add['name']}: {add['units']}単位")
            lines.append(f"- 処遇改善加算: {d['treatment_improvement']:,}円")
            lines.append(f"- **月額合計: {d['total_amount']:,}円**\n")
        elif t == "simulation":
            lines.append("**【採用シミュレーション結果】**")
            lines.append(f"- 対象: {d['new_employee']['name']}（{d['new_employee']['position']}）")
            lines.append(f"- 現在の月次報酬: {d['current_monthly']:,}円")
            lines.append(f"- 採用後の月次報酬: {d['simulated_monthly']:,}円")
            diff = d["diff_monthly"]
            arrow = "+" if diff >= 0 else ""
            lines.append(f"- **差分: {arrow}{diff:,}円/月（年間 {arrow}{d['diff_annual']:,}円）**\n")
        elif t == "error":
            lines.append(f"エラー: {d}")
    return "\n".join(lines)


# --- メッセージ処理 ---

def parse_ai_response(text):
    def extract(data):
        return (data.get("reply", ""),
                data.get("task_ops", []),
                data.get("schedule_ops", []),
                data.get("hr_ops", []),
                data.get("revenue_ops", []),
                data.get("doc_ops", []))
    try:
        data = json.loads(text)
        if isinstance(data, dict) and "reply" in data:
            return extract(data)
    except (json.JSONDecodeError, TypeError):
        pass
    match = re.search(r'\{[\s\S]*"reply"[\s\S]*\}', text)
    if match:
        try:
            data = json.loads(match.group())
            if isinstance(data, dict) and "reply" in data:
                return extract(data)
        except (json.JSONDecodeError, TypeError):
            pass
    return text, [], [], [], [], []


# ==========================================
# Discord ダッシュボードUI
# ==========================================

PRIORITY_EMOJI = {"urgent": "🔴", "high": "🟠", "medium": "🟡", "low": "⚪"}


def get_deadline_color_and_icon(due_date_str):
    """期限日から色とアイコンを返す"""
    if not due_date_str:
        return discord.Color.greyple(), "📝"
    try:
        d = date.fromisoformat(str(due_date_str))
    except (ValueError, TypeError):
        return discord.Color.greyple(), "📝"
    today = date.today()
    diff = (d - today).days
    if diff < 0:
        return discord.Color.from_rgb(80, 0, 0), "🚨"
    if diff == 0:
        return discord.Color.red(), "🔥"
    if diff <= 2:
        return discord.Color.orange(), "⚠️"
    if diff <= 7:
        return discord.Color.gold(), "📌"
    return discord.Color.green(), "✅"


def categorize_tasks_by_period(tasks_list):
    """タスクを期間別に分類: 期限切れ/今日/今週/今月/来月/それ以降/期限なし"""
    today = date.today()
    # 今週の終わり（日曜日）
    week_end = today + timedelta(days=(6 - today.weekday()))
    # 今月の終わり
    if today.month == 12:
        month_end = date(today.year + 1, 1, 1) - timedelta(days=1)
        next_month_end = date(today.year + 1, 2, 1) - timedelta(days=1)
    elif today.month == 11:
        month_end = date(today.year, 12, 31)
        next_month_end = date(today.year + 1, 1, 31)
    else:
        month_end = date(today.year, today.month + 1, 1) - timedelta(days=1)
        next_month_end = date(today.year, today.month + 2, 1) - timedelta(days=1)

    buckets = {
        "overdue": [],    # 期限切れ
        "today": [],      # 今日
        "this_week": [],  # 今週（今日除く）
        "this_month": [], # 今月（今週除く）
        "next_month": [], # 来月
        "later": [],      # それ以降
        "no_date": [],    # 期限なし
    }

    for t in tasks_list:
        due_str = t.get("due_date")
        if not due_str:
            buckets["no_date"].append(t)
            continue
        try:
            d = date.fromisoformat(str(due_str))
        except (ValueError, TypeError):
            buckets["no_date"].append(t)
            continue
        if d < today:
            buckets["overdue"].append(t)
        elif d == today:
            buckets["today"].append(t)
        elif d <= week_end:
            buckets["this_week"].append(t)
        elif d <= month_end:
            buckets["this_month"].append(t)
        elif d <= next_month_end:
            buckets["next_month"].append(t)
        else:
            buckets["later"].append(t)

    return buckets


def build_tasks_embed(user_id, page=0, per_page=5):
    tasks_list = get_tasks(user_id)
    total = len(tasks_list)

    if not tasks_list:
        return discord.Embed(
            title="📋 タスクダッシュボード",
            description="未完了のタスクはありません。",
            color=discord.Color.greyple()
        )

    buckets = categorize_tasks_by_period(tasks_list)

    # ページングは全タスクのフラットリスト（期間別ソート済み）
    ordered = (
        buckets["overdue"] + buckets["today"] + buckets["this_week"]
        + buckets["this_month"] + buckets["next_month"]
        + buckets["later"] + buckets["no_date"]
    )

    start = page * per_page
    end = start + per_page
    visible = ordered[start:end]

    # 最も緊急なバケットの色でembed全体の色を決める
    if buckets["overdue"]:
        embed_color = discord.Color.from_rgb(80, 0, 0)
    elif buckets["today"]:
        embed_color = discord.Color.red()
    elif buckets["this_week"]:
        embed_color = discord.Color.orange()
    else:
        embed_color = discord.Color.blue()

    total_pages = max(1, (total + per_page - 1) // per_page)

    # ヘッダーサマリ
    summary_parts = []
    if buckets["overdue"]:
        summary_parts.append(f"🚨期限切れ:{len(buckets['overdue'])}")
    if buckets["today"]:
        summary_parts.append(f"🔥今日:{len(buckets['today'])}")
    if buckets["this_week"]:
        summary_parts.append(f"⚠️今週:{len(buckets['this_week'])}")
    if buckets["this_month"]:
        summary_parts.append(f"📌今月:{len(buckets['this_month'])}")
    if buckets["next_month"]:
        summary_parts.append(f"📅来月:{len(buckets['next_month'])}")
    if buckets["later"]:
        summary_parts.append(f"✅以降:{len(buckets['later'])}")
    if buckets["no_date"]:
        summary_parts.append(f"📝期限なし:{len(buckets['no_date'])}")

    embed = discord.Embed(
        title=f"📋 タスクダッシュボード ({start+1}〜{min(end, total)}/{total}件)",
        description=" ・ ".join(summary_parts) if summary_parts else "",
        color=embed_color,
        timestamp=datetime.now()
    )

    for t in visible:
        _, icon = get_deadline_color_and_icon(t.get("due_date"))
        pri = PRIORITY_EMOJI.get(t.get("priority", "medium"), "⚪")
        team = "👥" if t.get("is_team_task") else "👤"
        due = t.get("due_date") or "期限なし"
        cat = t.get("category") or "その他"
        embed.add_field(
            name=f"{icon} [#{t['id']}] {pri} {t['title']}",
            value=f"{team} 期限: **{due}** ・ カテゴリ: `{cat}`",
            inline=False
        )

    embed.set_footer(text=f"ページ {page+1}/{total_pages}")
    return embed


def build_schedules_embed(user_id, page=0, per_page=5):
    schedules_list = get_schedules(user_id)
    total = len(schedules_list)

    if not schedules_list:
        return discord.Embed(
            title="📅 スケジュールダッシュボード",
            description="今後の予定はありません。",
            color=discord.Color.greyple()
        )

    start = page * per_page
    end = start + per_page
    visible = schedules_list[start:end]
    total_pages = max(1, (total + per_page - 1) // per_page)

    embed = discord.Embed(
        title=f"📅 スケジュールダッシュボード ({start+1}〜{min(end, total)}/{total}件)",
        color=discord.Color.blue(),
        timestamp=datetime.now()
    )

    for s in visible:
        icon = "🎯" if s.get("schedule_type") == "milestone" else "📆"
        time_str = ""
        if s.get("start_time"):
            st = s["start_time"]
            time_str = f" {st}" if isinstance(st, str) else f" {st.strftime('%H:%M')}"
        loc = f" @{s['location']}" if s.get("location") else ""
        team = "👥" if s.get("is_team_event") else "👤"
        cat = s.get("category") or "その他"
        embed.add_field(
            name=f"{icon} [#{s['id']}] {s['title']}",
            value=f"{team} {s.get('start_date', '')}{time_str}{loc} ・ カテゴリ: `{cat}`",
            inline=False
        )

    embed.set_footer(text=f"ページ {page+1}/{total_pages}")
    return embed


def build_docs_embed(page=0, per_page=5, category=None):
    docs = get_documents(category=category)
    total = len(docs)

    if not docs:
        return discord.Embed(
            title="📁 資料ダッシュボード",
            description="登録されている資料はありません。",
            color=discord.Color.greyple()
        )

    start = page * per_page
    end = start + per_page
    visible = docs[start:end]
    total_pages = max(1, (total + per_page - 1) // per_page)

    title = f"📁 資料ダッシュボード ({start+1}〜{min(end, total)}/{total}件)"
    if category:
        title += f" - {category}"
    embed = discord.Embed(
        title=title,
        color=discord.Color.purple(),
        timestamp=datetime.now()
    )

    for d in visible:
        cat = d.get("category") or "未分類"
        tags_str = " ".join(f"`{t}`" for t in (d.get("tags") or []))
        size_kb = round((d.get("file_size") or 0) / 1024, 1) if d.get("file_size") else 0
        desc = d.get("description") or ""
        link = d.get("drive_web_link") or ""
        value_lines = [f"カテゴリ: `{cat}`"]
        if tags_str:
            value_lines.append(f"タグ: {tags_str}")
        if desc:
            value_lines.append(desc[:80])
        if size_kb:
            value_lines.append(f"サイズ: {size_kb} KB")
        if link:
            value_lines.append(f"[📎 開く]({link})")
        embed.add_field(
            name=f"📄 [#{d['id']}] {d['title']}",
            value="\n".join(value_lines),
            inline=False
        )

    embed.set_footer(text=f"ページ {page+1}/{total_pages}")
    return embed


def build_overview_embed(user_id):
    tasks_list = get_tasks(user_id)
    schedules_list = get_schedules(user_id)
    employees_list = get_employees(active_only=True)
    candidates_list = get_candidates()
    docs = get_documents(limit=500)

    today = date.today()
    urgent = [t for t in tasks_list if t.get("priority") in ("urgent", "high")]
    overdue = []
    for t in tasks_list:
        if t.get("due_date"):
            try:
                if date.fromisoformat(str(t["due_date"])) < today:
                    overdue.append(t)
            except (ValueError, TypeError):
                pass

    color = discord.Color.red() if overdue else discord.Color.blue()
    embed = discord.Embed(
        title="📊 Alagent ダッシュボード",
        color=color,
        timestamp=datetime.now()
    )
    embed.add_field(
        name="📋 タスク",
        value=f"全{len(tasks_list)}件 / 高優先: {len(urgent)} / 期限切れ: {len(overdue)}",
        inline=True
    )
    embed.add_field(
        name="📅 スケジュール",
        value=f"今後 {len(schedules_list)}件",
        inline=True
    )
    embed.add_field(
        name="👥 人材",
        value=f"従業員: {len(employees_list)}名 / 候補者: {len(candidates_list)}名",
        inline=True
    )
    embed.add_field(
        name="📁 資料",
        value=f"{len(docs)}件",
        inline=True
    )
    upcoming = schedules_list[:3]
    if upcoming:
        embed.add_field(
            name="直近の予定",
            value="\n".join(f"・ {s['title']} ({s.get('start_date','')})" for s in upcoming),
            inline=False
        )
    embed.set_footer(text="/tasks /schedule /docs で詳細表示")
    return embed


def make_task_complete_cb(user_id, task_id, page, per_page):
    async def cb(interaction):
        update_task(task_id, status="done")
        new_view = TaskDashboardView(user_id, page)
        new_embed = build_tasks_embed(user_id, page, per_page)
        await interaction.response.edit_message(embed=new_embed, view=new_view)
    return cb


def make_task_page_cb(user_id, new_page, per_page):
    async def cb(interaction):
        new_view = TaskDashboardView(user_id, new_page)
        new_embed = build_tasks_embed(user_id, new_page, per_page)
        await interaction.response.edit_message(embed=new_embed, view=new_view)
    return cb


class TaskDashboardView(View):
    def __init__(self, user_id, page=0):
        super().__init__(timeout=600)
        self.user_id = user_id
        self.page = page
        self.per_page = 5
        self._build_items()

    def _build_items(self):
        self.clear_items()
        tasks_list = get_tasks(self.user_id)
        total = len(tasks_list)
        start = self.page * self.per_page
        end = start + self.per_page
        visible = tasks_list[start:end]

        for t in visible:
            btn = Button(
                label=f"✅ #{t['id']}",
                style=discord.ButtonStyle.success,
                row=0
            )
            btn.callback = make_task_complete_cb(self.user_id, t['id'], self.page, self.per_page)
            self.add_item(btn)

        if total > self.per_page:
            prev_btn = Button(
                label="◀ 前",
                style=discord.ButtonStyle.secondary,
                disabled=(self.page == 0),
                row=1
            )
            prev_btn.callback = make_task_page_cb(self.user_id, max(0, self.page - 1), self.per_page)
            self.add_item(prev_btn)

            next_btn = Button(
                label="次 ▶",
                style=discord.ButtonStyle.secondary,
                disabled=(end >= total),
                row=1
            )
            next_btn.callback = make_task_page_cb(self.user_id, self.page + 1, self.per_page)
            self.add_item(next_btn)

        refresh_btn = Button(label="🔄 更新", style=discord.ButtonStyle.primary, row=1)
        refresh_btn.callback = make_task_page_cb(self.user_id, self.page, self.per_page)
        self.add_item(refresh_btn)


def make_schedule_delete_cb(user_id, schedule_id, page, per_page):
    async def cb(interaction):
        delete_schedule(schedule_id)
        new_view = ScheduleDashboardView(user_id, page)
        new_embed = build_schedules_embed(user_id, page, per_page)
        await interaction.response.edit_message(embed=new_embed, view=new_view)
    return cb


def make_schedule_page_cb(user_id, new_page, per_page):
    async def cb(interaction):
        new_view = ScheduleDashboardView(user_id, new_page)
        new_embed = build_schedules_embed(user_id, new_page, per_page)
        await interaction.response.edit_message(embed=new_embed, view=new_view)
    return cb


class ScheduleDashboardView(View):
    def __init__(self, user_id, page=0):
        super().__init__(timeout=600)
        self.user_id = user_id
        self.page = page
        self.per_page = 5
        self._build_items()

    def _build_items(self):
        self.clear_items()
        schedules_list = get_schedules(self.user_id)
        total = len(schedules_list)
        start = self.page * self.per_page
        end = start + self.per_page
        visible = schedules_list[start:end]

        for s in visible:
            btn = Button(label=f"🗑 #{s['id']}", style=discord.ButtonStyle.danger, row=0)
            btn.callback = make_schedule_delete_cb(self.user_id, s['id'], self.page, self.per_page)
            self.add_item(btn)

        if total > self.per_page:
            prev_btn = Button(label="◀ 前", style=discord.ButtonStyle.secondary,
                              disabled=(self.page == 0), row=1)
            prev_btn.callback = make_schedule_page_cb(self.user_id, max(0, self.page - 1), self.per_page)
            self.add_item(prev_btn)

            next_btn = Button(label="次 ▶", style=discord.ButtonStyle.secondary,
                              disabled=(end >= total), row=1)
            next_btn.callback = make_schedule_page_cb(self.user_id, self.page + 1, self.per_page)
            self.add_item(next_btn)

        refresh_btn = Button(label="🔄 更新", style=discord.ButtonStyle.primary, row=1)
        refresh_btn.callback = make_schedule_page_cb(self.user_id, self.page, self.per_page)
        self.add_item(refresh_btn)


class DocsDashboardView(View):
    def __init__(self, page=0, category=None):
        super().__init__(timeout=600)
        self.page = page
        self.per_page = 5
        self.category = category
        self._build_items()

    def _build_items(self):
        self.clear_items()
        docs = get_documents(category=self.category)
        total = len(docs)

        if total > self.per_page:
            prev_btn = Button(label="◀ 前", style=discord.ButtonStyle.secondary,
                              disabled=(self.page == 0), row=0)

            async def prev_cb(interaction):
                new_view = DocsDashboardView(self.page - 1, self.category)
                new_embed = build_docs_embed(self.page - 1, self.per_page, self.category)
                await interaction.response.edit_message(embed=new_embed, view=new_view)

            prev_btn.callback = prev_cb
            self.add_item(prev_btn)

            next_btn = Button(label="次 ▶", style=discord.ButtonStyle.secondary,
                              disabled=((self.page + 1) * self.per_page >= total), row=0)

            async def next_cb(interaction):
                new_view = DocsDashboardView(self.page + 1, self.category)
                new_embed = build_docs_embed(self.page + 1, self.per_page, self.category)
                await interaction.response.edit_message(embed=new_embed, view=new_view)

            next_btn.callback = next_cb
            self.add_item(next_btn)

        refresh_btn = Button(label="🔄 更新", style=discord.ButtonStyle.primary, row=0)

        async def refresh_cb(interaction):
            new_view = DocsDashboardView(self.page, self.category)
            new_embed = build_docs_embed(self.page, self.per_page, self.category)
            await interaction.response.edit_message(embed=new_embed, view=new_view)

        refresh_btn.callback = refresh_cb
        self.add_item(refresh_btn)

        scan_btn = Button(label="🔍 Driveスキャン", style=discord.ButtonStyle.secondary, row=0)

        async def scan_cb(interaction):
            await interaction.response.defer()
            result = scan_and_import_drive(str(interaction.user.id))
            new_view = DocsDashboardView(0, self.category)
            new_embed = build_docs_embed(0, self.per_page, self.category)
            await interaction.edit_original_response(
                content=f"📥 スキャン完了: 取り込み {result['imported']}件 / スキップ {result['skipped']}件",
                embed=new_embed,
                view=new_view
            )

        scan_btn.callback = scan_cb
        self.add_item(scan_btn)


# --- スラッシュコマンド ---

@tree.command(name="tasks", description="タスクダッシュボードを表示")
async def tasks_command(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    embed = build_tasks_embed(user_id, page=0)
    view = TaskDashboardView(user_id, page=0)
    await interaction.response.send_message(embed=embed, view=view)


@tree.command(name="schedule", description="スケジュールダッシュボードを表示")
async def schedule_command(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    embed = build_schedules_embed(user_id, page=0)
    view = ScheduleDashboardView(user_id, page=0)
    await interaction.response.send_message(embed=embed, view=view)


@tree.command(name="docs", description="資料ダッシュボードを表示")
@app_commands.describe(category="カテゴリで絞り込み（任意）")
async def docs_command(interaction: discord.Interaction, category: str = None):
    embed = build_docs_embed(page=0, category=category)
    view = DocsDashboardView(page=0, category=category)
    await interaction.response.send_message(embed=embed, view=view)


@tree.command(name="sync", description="Google DriveとDBを同期（Drive→DB一方向）")
async def sync_command(interaction: discord.Interaction):
    await interaction.response.defer()
    user_id = str(interaction.user.id)
    result = sync_drive_to_db(user_id)
    embed = discord.Embed(
        title="🔄 Drive同期完了",
        color=discord.Color.green(),
        timestamp=datetime.now()
    )
    embed.add_field(name="追加", value=f"{result['added']}件", inline=True)
    embed.add_field(name="更新", value=f"{result['updated']}件", inline=True)
    embed.add_field(name="削除", value=f"{result['removed']}件", inline=True)
    embed.add_field(name="エラー", value=f"{result['errors']}件", inline=True)
    embed.add_field(name="Drive総ファイル数", value=f"{result['total']}件", inline=True)
    await interaction.followup.send(embed=embed)


@tree.command(name="dashboard", description="全体ダッシュボードを表示")
async def dashboard_command(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    embed = build_overview_embed(user_id)
    await interaction.response.send_message(embed=embed)


@tasks.loop(minutes=15)
async def drive_sync_loop():
    """15分ごとにDrive→DBの自動同期"""
    try:
        result = sync_drive_to_db("auto_sync")
        if result["added"] or result["updated"] or result["removed"]:
            print(f"[Auto Sync] +{result['added']} *{result['updated']} -{result['removed']}")
    except Exception as e:
        print(f"[Auto Sync Error] {e}")


@client.event
async def on_ready():
    print(f"Alagent起動完了: {client.user}")
    try:
        synced = await tree.sync()
        print(f"[Slash] {len(synced)}個のコマンドを同期")
    except Exception as e:
        print(f"[Slash Sync Error] {e}")
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        get_drive_service()  # 初期化を試みる
        drive_sync_loop.start()
        print("[Drive Sync] 15分おき自動同期ループ開始")
    if REMINDER_CHANNEL_ID:
        reminder_loop.start()
        print(f"[Reminder] 自動通知ループ開始 (channel={REMINDER_CHANNEL_ID})")


def _fmt_task_line(t):
    due = t.get("due_date", "")
    if isinstance(due, date):
        due = due.isoformat()
    pri = PRIORITY_EMOJI.get(t.get("priority", "medium"), "⚪")
    return f"  {pri} {t['title']}（期限: {due}）"


def _fmt_schedule_line(s):
    time_str = ""
    if s.get("start_time"):
        st = s["start_time"]
        time_str = f" {st.strftime('%H:%M') if hasattr(st, 'strftime') else st}"
    loc = f" @{s['location']}" if s.get("location") else ""
    return f"  📆 {s['title']}{time_str}{loc}"


@tasks.loop(hours=1)
async def reminder_loop():
    now = datetime.now()
    if now.hour != 8:
        return
    try:
        r = get_upcoming_reminders()
        has_content = any([
            r["overdue_tasks"], r["today_tasks"], r["tomorrow_tasks"],
            r["week_tasks"], r["today_schedules"], r["tomorrow_schedules"],
            r["week_schedules"]
        ])
        if not has_content:
            return

        channel = client.get_channel(int(REMINDER_CHANNEL_ID))
        if not channel:
            print(f"[Reminder] チャンネル {REMINDER_CHANNEL_ID} が見つかりません")
            return

        lines = ["**📋 おはようございます！本日のブリーフィングです。**\n"]

        if r["overdue_tasks"]:
            lines.append(f"🚨 **期限切れ（{len(r['overdue_tasks'])}件）**")
            for t in r["overdue_tasks"]:
                lines.append(_fmt_task_line(t))
            lines.append("")

        if r["today_tasks"]:
            lines.append(f"🔥 **今日が期限（{len(r['today_tasks'])}件）**")
            for t in r["today_tasks"]:
                lines.append(_fmt_task_line(t))
            lines.append("")

        if r["today_schedules"]:
            lines.append(f"📅 **今日の予定（{len(r['today_schedules'])}件）**")
            for s in r["today_schedules"]:
                lines.append(_fmt_schedule_line(s))
            lines.append("")

        if r["tomorrow_tasks"] or r["tomorrow_schedules"]:
            lines.append("**--- 明日 ---**")
            for t in r["tomorrow_tasks"]:
                lines.append(_fmt_task_line(t))
            for s in r["tomorrow_schedules"]:
                lines.append(_fmt_schedule_line(s))
            lines.append("")

        if r["week_tasks"] or r["week_schedules"]:
            lines.append("**--- 今週残り ---**")
            for t in r["week_tasks"]:
                lines.append(_fmt_task_line(t))
            for s in r["week_schedules"]:
                lines.append(_fmt_schedule_line(s))
            lines.append("")

        await channel.send("\n".join(lines)[:2000])
        total_items = sum(len(r[k]) for k in r)
        print(f"[Reminder] 通知送信完了: {total_items}件")
    except Exception as e:
        print(f"[Reminder Error] {e}")


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    user_id = str(message.author.id)
    channel_id = str(message.channel.id)

    if message.content.strip() == "/clear":
        clear_history(user_id)
        await message.reply("会話履歴をクリアしました。")
        return

    save_message(user_id, channel_id, "user", message.content)

    # --- 添付ファイル処理（Driveアップロード + 中身読み取り + 永続化） ---
    uploaded_docs_info = []
    if message.attachments:
        async with message.channel.typing():
            for att in message.attachments:
                try:
                    file_bytes = await att.read()
                    mime = att.content_type or "application/octet-stream"
                    raw_filename = att.filename
                    # 優先順: URL末尾から復元 > att.filename
                    url_filename = None
                    try:
                        att_url = getattr(att, "url", None) or ""
                        if att_url:
                            path = att_url.split("?")[0]
                            last = path.rsplit("/", 1)[-1]
                            url_filename = unquote(last) if last else None
                    except Exception:
                        url_filename = None

                    candidates = []
                    for cand in (url_filename, raw_filename):
                        if not cand:
                            continue
                        try:
                            candidates.append(unquote(cand))
                        except Exception:
                            candidates.append(cand)

                    # 日本語を含むものを優先
                    filename = None
                    for cand in candidates:
                        if not is_mangled_filename(cand):
                            filename = cand
                            break
                    if not filename:
                        filename = candidates[0] if candidates else raw_filename

                    # それでも崩れているならメッセージ本文から推測
                    if is_mangled_filename(filename) and message.content.strip():
                        inferred = infer_filename_from_message(message.content, filename)
                        print(f"[Attachment] mangled -> inferred: {filename!r} -> {inferred!r}")
                        filename = inferred

                    print(f"[Attachment] raw={raw_filename!r} url_name={url_filename!r} final={filename!r}")
                    doc_info = {"filename": filename, "mime_type": mime}

                    # テキスト系ファイルの中身を読み取る
                    text_extensions = (".txt", ".md", ".csv", ".json", ".log", ".yaml", ".yml", ".py", ".js", ".html", ".xml", ".tsv")
                    is_text = (
                        mime.startswith("text/") or
                        mime in ("application/json", "application/xml") or
                        filename.lower().endswith(text_extensions)
                    )
                    full_text = None
                    if is_text:
                        try:
                            full_text = file_bytes.decode("utf-8")
                        except UnicodeDecodeError:
                            try:
                                full_text = file_bytes.decode("shift_jis")
                            except UnicodeDecodeError:
                                full_text = None
                        if full_text:
                            # payloadには8000文字まで
                            doc_info["text_content"] = full_text[:8000]
                            if len(full_text) > 8000:
                                doc_info["text_truncated"] = True

                    # Driveに保存 + DB登録（環境変数が設定されている場合のみDriveアップ、DBは常に登録）
                    drive_file_id = None
                    drive_web_link = None
                    file_size = len(file_bytes)
                    if GOOGLE_SERVICE_ACCOUNT_JSON:
                        result = upload_to_drive(file_bytes, filename, mime_type=mime)
                        if result:
                            drive_file_id = result["file_id"]
                            drive_web_link = result["web_link"]
                            file_size = result["size"]
                            mime = result["mime_type"]

                    # text_contentを含めてDBに永続化
                    try:
                        doc_id = create_document(
                            user_id=user_id,
                            title=filename,
                            drive_file_id=drive_file_id,
                            drive_web_link=drive_web_link,
                            file_name=filename,
                            mime_type=mime,
                            file_size=file_size,
                            text_content=full_text  # 全文保存
                        )
                        doc_info["doc_id"] = doc_id
                        if drive_web_link:
                            doc_info["link"] = drive_web_link
                    except Exception as e:
                        print(f"[DB Insert Error] {e}")

                    uploaded_docs_info.append(doc_info)
                except Exception as e:
                    print(f"[Attachment Error] {e}")

    history = get_history(user_id)
    current_tasks = get_tasks(user_id)
    current_schedules = get_schedules(user_id)
    current_candidates = get_candidates()
    current_employees = get_employees(active_only=True)
    qualifications_master = get_qualifications()
    current_documents = get_documents(limit=30)

    async with message.channel.typing():
        try:
            payload = {
                "content": message.content,
                "user_id": user_id,
                "username": str(message.author),
                "channel_id": channel_id,
                "history": history,
                "tasks": current_tasks,
                "schedules": current_schedules,
                "candidates": current_candidates,
                "employees": current_employees,
                "qualifications_master": qualifications_master,
                "documents": current_documents,
                "uploaded_docs": uploaded_docs_info
            }
            r = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=30)
            print(f"[n8n] status={r.status_code}")

            if r.status_code == 200:
                data = r.json()
                raw_reply = data.get("text", "") or data.get("message", "") or str(data)
                reply, task_ops, schedule_ops, hr_ops, revenue_ops, doc_ops = parse_ai_response(raw_reply)

                if task_ops:
                    op_results = execute_task_ops(user_id, task_ops)
                    print(f"[TaskOps] {op_results}")

                if schedule_ops:
                    op_results = execute_schedule_ops(user_id, schedule_ops)
                    print(f"[ScheduleOps] {op_results}")

                if hr_ops:
                    op_results = execute_hr_ops(user_id, hr_ops)
                    print(f"[HROps] {op_results}")

                if doc_ops:
                    op_results = execute_doc_ops(user_id, doc_ops)
                    print(f"[DocOps] {op_results}")

                if revenue_ops:
                    rev_results = execute_revenue_ops(user_id, revenue_ops)
                    print(f"[RevenueOps] {len(rev_results)} results")
                    if rev_results:
                        formatted = format_revenue_result(rev_results)
                        if formatted:
                            reply = (reply + "\n\n" + formatted) if reply else formatted

                # 添付ファイル登録結果をreplyに追記
                saved_docs = [d for d in uploaded_docs_info if d.get("doc_id")]
                if saved_docs:
                    lines = ["\n\n**資料を登録しました:**"]
                    for d in saved_docs:
                        link_part = f" → [開く]({d['link']})" if d.get("link") else ""
                        lines.append(f"- [Doc#{d['doc_id']}] {d['filename']}{link_part}")
                    reply = (reply or "") + "\n".join(lines)

                if reply:
                    save_message(user_id, channel_id, "assistant", reply)
                    await message.reply(reply[:2000])
                else:
                    await message.reply("処理完了しました。")
            else:
                print(f"[n8n] error={r.text[:200]}")
                await message.reply(f"エラーが発生しました（status={r.status_code}）")

        except Exception as e:
            print(f"[Error] {e}")
            await message.reply(f"エラーが発生しました: {str(e)[:100]}")


init_db()
client.run(DISCORD_TOKEN)
