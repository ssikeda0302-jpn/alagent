import discord
from discord.ext import tasks
import os
import json
import re
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import date, datetime, time, timedelta
import asyncio

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
N8N_WEBHOOK_URL = os.environ["N8N_WEBHOOK_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]
REMINDER_CHANNEL_ID = os.environ.get("REMINDER_CHANNEL_ID", "")
MAX_HISTORY = 20

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)


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
    tomorrow = (datetime.now() + timedelta(days=1)).date()
    today = datetime.now().date()
    results = {"tasks": [], "schedules": []}
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 明日期限のタスク
            cur.execute(
                """SELECT id, title, priority, due_date, assignee_user_id, created_by_user_id
                   FROM tasks WHERE due_date = %s AND status != 'done'""",
                (tomorrow,)
            )
            results["tasks"] = cur.fetchall()
            # 今日期限で未完了のタスク（当日警告）
            cur.execute(
                """SELECT id, title, priority, due_date, assignee_user_id, created_by_user_id
                   FROM tasks WHERE due_date = %s AND status != 'done'""",
                (today,)
            )
            results["tasks"].extend(cur.fetchall())
            # 明日の予定
            cur.execute(
                """SELECT id, title, schedule_type, start_date, start_time, location,
                          created_by_user_id
                   FROM schedules WHERE start_date = %s AND reminder_sent = FALSE""",
                (tomorrow,)
            )
            results["schedules"] = cur.fetchall()
            # リマインダー送信済みに更新
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
    # まずそのままJSON解析
    try:
        data = json.loads(text)
        if isinstance(data, dict) and "reply" in data:
            return (data.get("reply", ""),
                    data.get("task_ops", []),
                    data.get("schedule_ops", []),
                    data.get("hr_ops", []),
                    data.get("revenue_ops", []))
    except (json.JSONDecodeError, TypeError):
        pass
    # テキスト中からJSON部分を抽出して解析
    match = re.search(r'\{[\s\S]*"reply"[\s\S]*\}', text)
    if match:
        try:
            data = json.loads(match.group())
            if isinstance(data, dict) and "reply" in data:
                return (data.get("reply", ""),
                        data.get("task_ops", []),
                        data.get("schedule_ops", []),
                        data.get("hr_ops", []),
                        data.get("revenue_ops", []))
        except (json.JSONDecodeError, TypeError):
            pass
    return text, [], [], [], []


@client.event
async def on_ready():
    print(f"Alagent起動完了: {client.user}")
    if REMINDER_CHANNEL_ID:
        reminder_loop.start()
        print(f"[Reminder] 自動通知ループ開始 (channel={REMINDER_CHANNEL_ID})")


@tasks.loop(hours=1)
async def reminder_loop():
    now = datetime.now()
    # 毎日8時台に実行（JST想定、Railwayのタイムゾーン設定に依存）
    if now.hour != 8:
        return
    try:
        reminders = get_upcoming_reminders()
        if not reminders["tasks"] and not reminders["schedules"]:
            return

        channel = client.get_channel(int(REMINDER_CHANNEL_ID))
        if not channel:
            print(f"[Reminder] チャンネル {REMINDER_CHANNEL_ID} が見つかりません")
            return

        lines = ["**リマインダー通知**\n"]

        if reminders["tasks"]:
            lines.append("**タスク（期限間近）:**")
            for t in reminders["tasks"]:
                due = t.get("due_date", "")
                if isinstance(due, date):
                    due = due.isoformat()
                lines.append(f"- {t['title']}（期限: {due}、優先度: {t['priority']}）")
            lines.append("")

        if reminders["schedules"]:
            lines.append("**明日の予定:**")
            for s in reminders["schedules"]:
                time_str = ""
                if s.get("start_time"):
                    st = s["start_time"]
                    time_str = f" {st.strftime('%H:%M') if hasattr(st, 'strftime') else st}"
                loc = f" @{s['location']}" if s.get("location") else ""
                lines.append(f"- {s['title']}{time_str}{loc}")

        await channel.send("\n".join(lines))
        print(f"[Reminder] 通知送信完了: tasks={len(reminders['tasks'])}, schedules={len(reminders['schedules'])}")
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
    history = get_history(user_id)
    current_tasks = get_tasks(user_id)
    current_schedules = get_schedules(user_id)
    current_candidates = get_candidates()
    current_employees = get_employees(active_only=True)
    qualifications_master = get_qualifications()

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
                "qualifications_master": qualifications_master
            }
            r = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=30)
            print(f"[n8n] status={r.status_code}")

            if r.status_code == 200:
                data = r.json()
                raw_reply = data.get("text", "") or data.get("message", "") or str(data)
                reply, task_ops, schedule_ops, hr_ops, revenue_ops = parse_ai_response(raw_reply)

                if task_ops:
                    op_results = execute_task_ops(user_id, task_ops)
                    print(f"[TaskOps] {op_results}")

                if schedule_ops:
                    op_results = execute_schedule_ops(user_id, schedule_ops)
                    print(f"[ScheduleOps] {op_results}")

                if hr_ops:
                    op_results = execute_hr_ops(user_id, hr_ops)
                    print(f"[HROps] {op_results}")

                if revenue_ops:
                    rev_results = execute_revenue_ops(user_id, revenue_ops)
                    print(f"[RevenueOps] {len(rev_results)} results")
                    # 報酬算定結果をreplyに追記
                    if rev_results:
                        formatted = format_revenue_result(rev_results)
                        if formatted:
                            reply = (reply + "\n\n" + formatted) if reply else formatted

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
