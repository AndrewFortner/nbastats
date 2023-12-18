import pandas as pd
import json
from collections import defaultdict
from tqdm import tqdm

# Read the CSV file
file_path = 'nbadata/games_details.csv'
df = pd.read_csv(file_path)

# Initialize a defaultdict to store the relationship data
relationship_table = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

# Iterate over each game with a progress bar
for game_id, game_data in tqdm(df.groupby('GAME_ID'), desc="Processing games"):
    # Group by team
    teams = game_data.groupby('TEAM_ID')

    # If there are not exactly two teams, skip this game (to handle edge cases)
    if len(teams) != 2:
        continue

    # Extract player IDs for each team
    team1_players = set(teams.get_group(next(iter(teams.groups))).PLAYER_ID)
    team2_players = set(teams.get_group(next(iter(reversed(teams.groups)))).PLAYER_ID)

    # For each pair of players from different teams, add the game_id to their relationship
    for player1 in team1_players:
        for player2 in team2_players:
            relationship_table[player1][player2]['game_ids'].append(game_id)
            relationship_table[player2][player1]['game_ids'].append(game_id)

            # You can add more features here as needed
            # Example: relationship_table[player1][player2]['another_feature'].append(some_value)

# add another feature to the relationship table to store the number of games played together
for player1 in relationship_table:
    for player2 in relationship_table[player1]:
        relationship_table[player1][player2]['num_games'] = len(relationship_table[player1][player2]['game_ids'])

# Convert the defaultdict to a regular dict for easier use
relationship_table = {k: {k2: dict(v2) for k2, v2 in v.items()} for k, v in relationship_table.items()}

# Save the output to a file
with open('nbaanalytics/player_relationships.json', 'w') as f:
    json.dump(relationship_table, f)

print("Relationship table created and saved to player_relationships.json.")
