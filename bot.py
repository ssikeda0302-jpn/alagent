import discord
import os
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

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
        conn.commit()
    print("[DB] テーブル初期化完了")


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

    async with message.channel.typing():
        try:
            payload = {
                "content": message.content,
                "user_id": user_id,
                "username": str(message.author),
                "channel_id": channel_id,
                "history": history
            }
            r = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=30)
            print(f"[n8n] status={r.status_code}")

            if r.status_code == 200:
                data = r.json()
                reply = data.get("text", "") or data.get("message", "") or str(data)
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
