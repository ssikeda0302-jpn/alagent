import discord
import os
import requests
from collections import defaultdict

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
N8N_WEBHOOK_URL = os.environ["N8N_WEBHOOK_URL"]

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

conversation_history = defaultdict(list)
MAX_HISTORY = 20

@client.event
async def on_ready():
    print(f"Alagent起動完了: {client.user}")

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    user_id = str(message.author.id)

    if message.content.strip() == "/clear":
        conversation_history[user_id] = []
        await message.reply("会話履歴をクリアしました。")
        return

    conversation_history[user_id].append({
        "role": "user",
        "content": message.content
    })

    if len(conversation_history[user_id]) > MAX_HISTORY:
        conversation_history[user_id] = conversation_history[user_id][-MAX_HISTORY:]

    async with message.channel.typing():
        try:
            payload = {
                "content": message.content,
                "user_id": user_id,
                "username": str(message.author),
                "channel_id": str(message.channel.id),
                "history": conversation_history[user_id]
            }
            r = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=30)
            print(f"[n8n] status={r.status_code}")

            if r.status_code == 200:
                data = r.json()
                reply = data.get("text", "") or data.get("message", "") or str(data)
                if reply:
                    conversation_history[user_id].append({
                        "role": "assistant",
                        "content": reply
                    })
                    await message.reply(reply)
                else:
                    await message.reply("処理完了しました。")
            else:
                print(f"[n8n] error={r.text[:200]}")
                await message.reply(f"エラーが発生しました（status={r.status_code}）")

        except Exception as e:
            print(f"[Error] {e}")
            await message.reply(f"エラーが発生しました: {str(e)[:100]}")

client.run(DISCORD_TOKEN)
