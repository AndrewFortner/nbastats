import pandas as pd
from tqdm import tqdm

# Step 1: Load your data
# Replace 'your_filepath' with the path to your CSV file
games_details = pd.read_csv('nbadata/games_details.csv')
# Replace 'your_player_mapping_filepath' with the path to your player mapping file
player_mapping = pd.read_json('nbaanalytics/player_relationships.json')

# filter out records with num_games < n
def filter_player_mapping(player_mapping, n):
    return player_mapping.applymap(lambda x: x if type(x) != type(float('nan')) and x['num_games'] >= n else None)

# Step 2: Filter the player_mapping table
# Filter out records with num_games < 10
player_mapping = filter_player_mapping(player_mapping, 10)

# Function to filter games_details for a pair of players and return their stats
def get_player_pair_stats(player1_id, player2_id, games_details):
    games_player1 = games_details[(games_details['PLAYER_ID'] == player1_id) & (games_details['GAME_ID'].isin(player_mapping[player1_id][player2_id]['game_ids']))]
    games_player2 = games_details[(games_details['PLAYER_ID'] == player2_id) & (games_details['GAME_ID'].isin(player_mapping[player1_id][player2_id]['game_ids']))]

    relevant_stats = ['REB', 'AST', 'STL', 'PTS']
    player1_stats = games_player1[relevant_stats]
    player2_stats = games_player2[relevant_stats]

    return player1_stats, player2_stats

# Step 3 & 4: Calculate Correlations
correlation_table = {}
for player1_id in tqdm(player_mapping.index, desc="Calculating correlations"):
    for player2_id in player_mapping[player1_id].index:
        # skip if player_mapping[player1_id][player2_id] is nan (i.e. no games played together)
        if not player_mapping[player1_id][player2_id]:
            continue
        player1_stats, player2_stats = get_player_pair_stats(int(player1_id), int(player2_id), games_details)

        # Calculate correlation and store in table
        correlation = player1_stats.corrwith(player2_stats, axis=0)
        correlation_table[(player1_id, player2_id)] = correlation

# Convert the correlation table to a DataFrame for better visualization
correlation_df = pd.DataFrame(correlation_table)

correlation_df.to_csv('nbaanalytics/player_correlations.csv', index=True)

print("Correlation DataFrame saved as 'player_correlations.csv'")