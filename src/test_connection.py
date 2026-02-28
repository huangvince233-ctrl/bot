import os
import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')

async def test():
    print("Testing Bot Client...")
    bot = TelegramClient('data/sessions/test_bot', API_ID, API_HASH)
    try:
        await bot.start(bot_token=BOT_TOKEN)
        me = await bot.get_me()
        print(f"Successfully connected as @{me.username}")
        await bot.disconnect()
    except Exception as e:
        print(f"Bot Client Connection Failed: {e}")

    print("\nTesting User Client...")
    user = TelegramClient('data/sessions/copilot_user', API_ID, API_HASH)
    try:
        await user.connect()
        if not await user.is_user_authorized():
            print("User Client is not authorized.")
        else:
            me = await user.get_me()
            print(f"Successfully connected as {getattr(me, 'first_name', 'User')}")
        await user.disconnect()
    except Exception as e:
        print(f"User Client Connection Failed: {e}")

if __name__ == "__main__":
    asyncio.run(test())
