import discord
import anthropic
import os
import requests

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)
anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

SYSTEM_PROMPT = """あなたは児童発達支援・放課後等デイサービスの事業運営を支援するAIエージェントです。
開業目標は2026年です。
主要タスク：指定申請、物件選定、スタッフ採用、備品調達、保護者向け広報
必ず日本語で簡潔に回答してください。"""

def record_to_notion(title, result):
    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    data = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": {
            "名前": {"title": [{"text": {"content": title[:100]}}]},
            "ステータス": {"status": {"name": "完了"}},
            "担当Worker": {"select": {"name": "タスク管理"}},
            "優先度": {"select": {"name": "中"}},
            "実行結果": {"rich_text": [{"text": {"content": result[:500]}}]}
        }
    }
    try:
        r = requests.post(url, headers=headers, json=data)
        print(f"Notion API status: {r.status_code}")
        if r.status_code != 200:
            print(f"Notion error: {r.text}")
        return r.status_code == 200
    except Exception as e:
        print(f"Notion error: {e}")
        return False

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
        success = record_to_notion(message.content, reply)

        if success:
            await message.reply(reply + "\n\n✅ Notionに記録しました")
        else:
            await message.reply(reply + "\n\n⚠️ Notion記録に失敗しました")

client.run(DISCORD_TOKEN)
