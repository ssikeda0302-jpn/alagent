import discord
import os
import json
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import date

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
N8N_WEBHOOK_URL = os.environ["N8N_WEBHOOK_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]
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


# --- タスク操作の実行 ---

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


# --- メッセージ処理 ---

def parse_ai_response(text):
    import re
    # まずそのままJSON解析
    try:
        data = json.loads(text)
        if isinstance(data, dict) and "reply" in data:
            return data.get("reply", ""), data.get("task_ops", [])
    except (json.JSONDecodeError, TypeError):
        pass
    # テキスト中からJSON部分を抽出して解析
    match = re.search(r'\{[\s\S]*"reply"[\s\S]*\}', text)
    if match:
        try:
            data = json.loads(match.group())
            if isinstance(data, dict) and "reply" in data:
                return data.get("reply", ""), data.get("task_ops", [])
        except (json.JSONDecodeError, TypeError):
            pass
    return text, []


@client.event
async def on_ready():
    print(f"Alagent起動完了: {client.user}")


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
    tasks = get_tasks(user_id)

    async with message.channel.typing():
        try:
            payload = {
                "content": message.content,
                "user_id": user_id,
                "username": str(message.author),
                "channel_id": channel_id,
                "history": history,
                "tasks": tasks
            }
            r = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=30)
            print(f"[n8n] status={r.status_code}")

            if r.status_code == 200:
                data = r.json()
                raw_reply = data.get("text", "") or data.get("message", "") or str(data)
                reply, task_ops = parse_ai_response(raw_reply)

                if task_ops:
                    op_results = execute_task_ops(user_id, task_ops)
                    print(f"[TaskOps] {op_results}")

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
