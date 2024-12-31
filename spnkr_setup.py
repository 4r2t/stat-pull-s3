import asyncio
import os
from aiohttp import ClientSession
from spnkr import AzureApp, authenticate_player

# Update or set an environment variable
def set_env_variable(key, value):
    # Persist the environment variable for new processes (Windows)
    os.system(f'setx {key} "{value}"')
    # Update the current session environment
    os.environ[key] = value

# Define your client_id, client_secret, and user credentials
client_id = os.getenv("GTCID")
client_secret = os.getenv("GTCSEC")
redirect_uri = "http://localhost"

async def main() -> None:
    app = AzureApp(client_id, client_secret, redirect_uri)

    async with ClientSession() as session:
        refresh_token = await authenticate_player(session, app)
    set_env_variable("RefreshToken", "{refresh_token}")
    print(f"Your refresh token is:\n{refresh_token}")
    await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())