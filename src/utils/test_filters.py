import asyncio
import os
from telethon import TelegramClient, functions
from dotenv import load_dotenv

load_dotenv()
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')

async def main():
    try:
        async with TelegramClient('data/copilot_user', API_ID, API_HASH) as client:
            filters_resp = await client(functions.messages.GetDialogFiltersRequest())
            all_filters = getattr(filters_resp, 'filters', filters_resp) if not isinstance(filters_resp, list) else filters_resp
            
            for f in all_filters:
                title = getattr(f, 'title', None)
                t_str = (title.text if hasattr(title, 'text') else str(title)) if title else ''
                print(f'Found Filter: "{t_str}"')
                if 'test' in t_str.lower() or '整理' in t_str:
                    print(f'  -> Inspecting include_peers for {t_str}:')
                    for peer in getattr(f, 'include_peers', []):
                        try:
                            e = await client.get_entity(peer)
                            e_type = type(e).__name__
                            e_title = getattr(e, "title", getattr(e, "first_name", getattr(e, "username", "NoName")))
                            print(f'      -> Entity: {e_type} "{e_title}" (ID: {e.id})')
                        except Exception as ex:
                            print(f'      -> Error: {ex}')
                            
                    print(f'  -> Inspecting pinned_peers for {t_str}:')
                    for peer in getattr(f, 'pinned_peers', []):
                        try:
                            e = await client.get_entity(peer)
                            e_type = type(e).__name__
                            e_title = getattr(e, "title", getattr(e, "first_name", getattr(e, "username", "NoName")))
                            print(f'      -> Pinned Entity: {e_type} "{e_title}" (ID: {e.id})')
                        except Exception as ex:
                            print(f'      -> Pinned Error: {ex}')
            print("Done")
    except Exception as e:
        print(f"Main Error: {e}")

if __name__ == '__main__':
    asyncio.run(main())
