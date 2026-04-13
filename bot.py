import discord
import anthropic
import os

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)
anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

SYSTEM_PROMPT = open("/app/workspace/HEARTBEAT.md").read()

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
        await message.reply(response.content[0].text)

client.run(DISCORD_TOKEN)
