import discord
import anthropic
import os
import requests
import json
from datetime import datetime

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)
anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

SYSTEM_PROMPT = open("/app/workspace/HEARTBEAT.md").read() + """

あなたはDiscordで指示を受けてタスクを実行するAIエージェントです。
ユーザーからタスクの依頼を受けたら：
1. タスクを実行または整理する
2. 結果をDiscordに返信する
3. タスクをNotionに記録する（record_task関数を使用）

必ず日本語で応答してください。
"""

def record_to_notion(title, status="未着手", worker="タスク管理", priority="中", result=""):
    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    data = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": {
            "名前": {"title": [{"text": {"content": title}}]},
            "ステータス": {"status": {"name": status}},
            "担当Worker": {"select": {"name": worker}},
            "優先度": {"select": {"name": priority}},
            "実行結果": {"rich_text": [{"text": {"content": result}}]}
        }
    }
    response = requests.post(url, headers=headers, json=data)
    return response.status_code == 200

@client.event
async def on_ready():
    print(f"Alagent起動完了: {client.user}")

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    async with message.channel.typing():
        response = anthropic_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": message.content}]
        )
        reply = response.content[0].text

        record_to_notion(
            title=message.content[:100],
            status="完了",
            result=reply[:200]
        )

        await message.reply(reply)

client.run(DISCORD_TOKEN)
