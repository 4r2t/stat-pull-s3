import asyncio
import os

from aiohttp import ClientSession
from spnkr import AzureApp, refresh_player_tokens

# Update or set an environment variable
def set_env_variable(key, value):
    # Persist the environment variable for new processes (Windows)
    os.system(f'setx {key} "{value}"')
    # Update the current session environment
    os.environ[key] = value

CLIENT_ID = os.getenv("GTCID")
CLIENT_SECRET = os.getenv("GTCSEC")
REDIRECT_URI = "http://localhost"
REFRESH_TOKEN = os.getenv("RefreshToken")

async def main() -> None:
    app = AzureApp(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI)

    async with ClientSession() as session:
        player = await refresh_player_tokens(session, app, REFRESH_TOKEN)

        # Update the environment variables
        set_env_variable("SpartanToken", player.spartan_token.token)
        print(f"Spartan token: {player.spartan_token.token}")  # Valid for 4 hours.

        set_env_variable("ClearanceToken", player.clearance_token.token)
        print(f"Clearance token: {player.clearance_token.token}")
        
        print(f"Xbox Live player ID (XUID): {player.player_id}")
        print(f"Xbox Live gamertag: {player.gamertag}")
        print(f"Xbox Live authorization: {player.xbl_authorization_header_value}")
    
    # Keep the loop alive for demonstration or delay if needed
    await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
