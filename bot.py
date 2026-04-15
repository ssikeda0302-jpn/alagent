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
        conn.commit()
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


# --- メッセージ処理 ---

def parse_ai_response(text):
    # まずそのままJSON解析
    try:
        data = json.loads(text)
        if isinstance(data, dict) and "reply" in data:
            return data.get("reply", ""), data.get("task_ops", []), data.get("schedule_ops", [])
    except (json.JSONDecodeError, TypeError):
        pass
    # テキスト中からJSON部分を抽出して解析
    match = re.search(r'\{[\s\S]*"reply"[\s\S]*\}', text)
    if match:
        try:
            data = json.loads(match.group())
            if isinstance(data, dict) and "reply" in data:
                return data.get("reply", ""), data.get("task_ops", []), data.get("schedule_ops", [])
        except (json.JSONDecodeError, TypeError):
            pass
    return text, [], []


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

    async with message.channel.typing():
        try:
            payload = {
                "content": message.content,
                "user_id": user_id,
                "username": str(message.author),
                "channel_id": channel_id,
                "history": history,
                "tasks": current_tasks,
                "schedules": current_schedules
            }
            r = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=30)
            print(f"[n8n] status={r.status_code}")

            if r.status_code == 200:
                data = r.json()
                raw_reply = data.get("text", "") or data.get("message", "") or str(data)
                reply, task_ops, schedule_ops = parse_ai_response(raw_reply)

                if task_ops:
                    op_results = execute_task_ops(user_id, task_ops)
                    print(f"[TaskOps] {op_results}")

                if schedule_ops:
                    op_results = execute_schedule_ops(user_id, schedule_ops)
                    print(f"[ScheduleOps] {op_results}")

                if reply:
                    save_message(user_id, channel_id, "assistant", reply)
                    await message.reply(reply)
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
