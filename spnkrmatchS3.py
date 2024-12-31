import pandas as pd
import asyncio
from aiohttp import ClientSession
from spnkr import HaloInfiniteClient
from spnkr.tools import MEDAL_NAME_MAP
from spnkr.services.profile import ProfileService
from spnkr.xuid import unwrap_xuid
import os

# Load Draft Positions from CSV
def load_draft_positions(csv_filename):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    csv_path = os.path.join(script_dir, csv_filename)
    df = pd.read_csv(csv_path)
    return {row['PlayerName']: row['DraftPos'] for _, row in df.iterrows()}

# Helper function to process the score field
def process_score(score):
    score_str = str(score).zfill(9)
    goal_assists = int(score_str[0])
    goals = int(score_str[1])
    plate_stops = int(score_str[2:4])
    sword_kills = int(score_str[4:6])
    bomb_hold_time = int(score_str[6:])
    return goal_assists, goals, plate_stops, sword_kills, bomb_hold_time

# Fetch gamertags for a list of Xbox User IDs (XUIDs)
async def fetch_gamertags(client, xuids):
    print(f"Fetching gamertags for XUIDs: {xuids}")
    
    # Unwrap XUIDs before passing them to the Halo client
    unwrapped_xuids = [unwrap_xuid(xuid) for xuid in xuids]
    resp = await client.profile.get_users_by_id(unwrapped_xuids)
    users_data = await resp.parse()

    gamertags_map = {}

    # Now iterate over the parsed user data
    for user_profile in users_data:
        gamertag = user_profile.gamertag  # Directly access gamertag
        xuid = user_profile.xuid
        print(f"XUID: {xuid} -> Gamertag: {gamertag}")
        gamertags_map[xuid] = gamertag

    print(f"Gamertag Map: {gamertags_map}")
    return gamertags_map

# Function to generate the match URL
def generate_match_url(gamertag, match_id):
    return f"https://www.halowaypoint.com/halo-infinite/players/{gamertag}/matches/{match_id}"

# Function to process a player's medals and map their names using MEDAL_NAME_MAP
def process_medals(core_stats):
    medal_counts = {}
    if hasattr(core_stats, 'medals'):
        for medal in core_stats.medals:
            # Correctly access 'name_id' and 'count' from AwardCount
            medal_name = MEDAL_NAME_MAP.get(medal.name_id, f"Medal {medal.name_id}")
            medal_counts[medal_name] = medal.count  # Use count for how many were earned
    return medal_counts

# Function to process a single match's data and return a DataFrame
async def process_match_data(match_json, client, draft_positions, match_id):
    players_data = []

    # Extract player information from match_json
    players = match_json.players  # Ensure this points to the correct part of the match data
    
    # Get the XUIDs for all players to resolve gamertags
    xuids = [unwrap_xuid(player.player_id) for player in players]
    gamertags_map = await fetch_gamertags(client, xuids)  # Fetch gamertags in one call

    # Iterate through each player to extract and calculate required fields
    for player in players:
        xuid = unwrap_xuid(player.player_id)  # Unwrap XUID before using
        gamertag = gamertags_map.get(xuid, xuid)  # Use XUID as fallback if gamertag not found

        print(f"Player XUID: {player.player_id}, Gamertag: {gamertag}")  # Debug print for each player

        last_team_id = player.last_team_id
        outcome = player.outcome
        team_stats = player.player_team_stats[0]
        core_stats = team_stats.stats.core_stats

        # Process score-related fields
        score = core_stats.score
        goal_assists, goals, plate_stops, sword_kills, bomb_hold_time = process_score(score)

        # Process medals (extracted from core_stats)
        medals = process_medals(core_stats)

        kills = core_stats.kills
        deaths = core_stats.deaths or 1
        power_weapon_kills = core_stats.power_weapon_kills
        time_played = player.participation_info.time_played.total_seconds()

        wins = 1 if outcome == 2 else 0
        losses = 1 if outcome == 3 else 0
        punches = kills - power_weapon_kills
        kd_ratio = kills / deaths
        game_time_seconds = round(time_played)

        draft_pos = draft_positions.get(gamertag, "N/A")
        match_url = generate_match_url(gamertag, match_id)

        player_data = {
            'PlayerId': gamertag,
            'DraftPos': draft_pos,
            'PlayerSub': None,
            'Wins': wins,
            'Losses': losses,
            'Score': score,
            'Damage Dealt': core_stats.damage_dealt,
            'Damage Taken': core_stats.damage_taken,
            'Goals': goals,
            'Goals Allowed': None,
            'BHT': bomb_hold_time,
            'BHT Allowed': None,
            'Punches': punches,
            'Goal Assists': goal_assists,
            'Plate Stops': plate_stops,
            'Plate Stops Allowed': None,
            'Kills': kills,
            'Sword Kills': sword_kills,
            'Deaths': deaths,
            'KDA': core_stats.kda,
            'KD Ratio': kd_ratio,
            'Assists': core_stats.assists,
            'Betrayals': core_stats.betrayals,
            'GameTime': game_time_seconds,
            'Killing Spree': medals.get('Killing Spree', 0),
            'Killing Frenzy': medals.get('Killing Frenzy', 0),
            'Running Riot': medals.get('Running Riot', 0),
            'Rampage': medals.get('Rampage', 0),
            'Grand Slams': medals.get('Grand Slam', 0),
            'Grand Slams Allowed': None,
            'Double Kill': medals.get('Double Kill', 0),
            'Triple Kill': medals.get('Triple Kill', 0),
            'Overkill': medals.get('Overkill', 0),
            'Killtacular': medals.get('Killtacular', 0),
            'Killtrocity': medals.get('Killtrocity', 0),
            'Killamanjaro': medals.get('Killamanjaro', 0),
            'Killtastrophe': medals.get('Killtastrophe', 0),
            'Killpocalypse': medals.get('Killpocalypse', 0),
            'Killionaire': medals.get('Killionaire', 0),
            'Extermination': medals.get('Extermination', 0),
            'Bulltrue': medals.get('Bulltrue', 0),
            'Ninja': medals.get('Ninja', 0),
            'Pancake': medals.get('Pancake', 0),
            'Whiplash': medals.get('Whiplash', 0),
            'Killjoy': medals.get('Killjoy', 0),
            'Harpoon': medals.get('Harpoon', 0),
            'Back Smack': medals.get('Back Smack', 0),
            'Spotter': medals.get('Spotter', 0),
            'Warrior': medals.get('Warrior', 0),
            'From the Grave': medals.get('From the Grave', 0),
            'Flawless Victory': medals.get('Flawless Victory', 0),
            'Boxer': medals.get('Boxer', 0),
            'URL': match_url,
            'MatchID': match_id,
            'Week': None,
            'Playoffs': None,
            'QF': None,
            'SF': None,
            'GF': None,
            'Team': None,
            'Opponent': None,
            'OldTeam': None
        }

        players_data.append(player_data)

    df = pd.DataFrame(players_data)
# Ensure score is padded to 9 digits before exporting
    df['Score'] = df['Score'].apply(lambda x: str(x).zfill(9))
# Sort dataframe by Wins=1 to sort output by winning team on top    
    df = df.sort_values(by='Wins', ascending=False)
    return df

# Function to process and save match data to CSV
async def process_match_to_csv(match_id, output_csv_file, client, draft_positions):
    match_stats = await client.stats.get_match_stats(match_id)
    match_json = await match_stats.parse()
    match_df = await process_match_data(match_json, client, draft_positions, match_id)
    match_df.to_csv(output_csv_file, index=False)
    print(f"Match data saved to {output_csv_file}")

# Main function
async def main(match_id, output_csv_file, draft_csv_filename):
    draft_positions = load_draft_positions(draft_csv_filename)

    async with ClientSession() as session:
        client = HaloInfiniteClient(
            session=session,
            spartan_token=os.getenv("SpartanToken"),
            clearance_token=os.getenv("ClearanceToken"),
            requests_per_second=5
        )

        await process_match_to_csv(match_id, output_csv_file, client, draft_positions)

# Command-line interface
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: python spnkr_process_match.py <match_id> <output_csv_file> <draft_csv_file>")
        sys.exit(1)

    match_id = sys.argv[1]
    output_csv_file = sys.argv[2]
    draft_csv_file = sys.argv[3]

    asyncio.run(main(match_id, output_csv_file, draft_csv_file))
