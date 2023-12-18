import pandas as pd
import gc

# Step 1: Load your data
# Replace 'your_filepath' with the path to your CSV file
games_details = pd.read_csv('nbadata/games_details.csv')
# Replace 'your_player_mapping_filepath' with the path to your player mapping file
player_mapping = pd.read_json('nbaanalytics/player_relationships.json')

# ----------------- Step 2: Filter the player_mapping table -----------------
games_details.set_index('GAME_ID', inplace=True)

# filter out records with num_games < n
def filter_player_mapping(player_mapping):
    return player_mapping.applymap(lambda x: x if type(x) != type(float('nan')) and x['num_games'] >= 1 else None)

# Step 2: Filter the player_mapping table
# Filter out records with num_games < 10
player_mapping = filter_player_mapping(player_mapping)

# ----------------- Step 3: Calculate Correlations -----------------

from concurrent.futures import ProcessPoolExecutor
import warnings
from tqdm import tqdm

# Disabling runtime warnings for numpy
warnings.filterwarnings('ignore', category=RuntimeWarning)
def process_pair_wrapper(pair):
    return process_player_pair(*pair)

# Function to filter games_details for a pair of players and return their stats
def get_player_pair_stats(player1_id, player2_id):
    games_player1 = games_details[(games_details['PLAYER_ID'] == player1_id) & (games_details.index.isin(player_mapping[player1_id][player2_id]['game_ids']))]
    games_player2 = games_details[(games_details['PLAYER_ID'] == player2_id) & (games_details.index.isin(player_mapping[player1_id][player2_id]['game_ids']))]

    relevant_stats = ['REB', 'AST', 'STL', 'PTS']
    player1_stats = games_player1[relevant_stats]
    player2_stats = games_player2[relevant_stats]

    return player1_stats, player2_stats

# Function to process each player pair - extracted from the double for loop
def process_player_pair(player1_id, player2_id):
    if not player_mapping[player1_id][player2_id]:
        return None
    player1_stats, player2_stats = get_player_pair_stats(int(player1_id), int(player2_id))

    nan_indices = player1_stats[player1_stats.isna().any(axis=1)].index.union(
        player2_stats[player2_stats.isna().any(axis=1)].index
    )

    player1_stats.drop(nan_indices, inplace=True)
    player2_stats.drop(nan_indices, inplace=True)

    correlations = pd.DataFrame(index=player1_stats.columns, columns=player2_stats.columns)
    for col1 in player1_stats.columns:
        for col2 in player2_stats.columns:
            correlations.at[col1, col2] = player1_stats[col1].corr(player2_stats[col2], method='spearman')

    player1_stats = None
    player2_stats = None

    return ((player1_id, player2_id), correlations)

# Function to parallelize processing with batch processing and limited workers
def parallel_process_player_pairs(player_mapping, batch_size=16000):
    correlation_table = {}

    player_pairs = list(set(sorted([(player1_id, player2_id)
                    for player1_id in player_mapping.index
                    for player2_id in player_mapping[player1_id].index])))
    print(len(player_pairs))

    for i in tqdm(range(0, len(player_pairs), batch_size)):
        batch_pairs = player_pairs[i:i + batch_size]
        with ProcessPoolExecutor() as executor:
            results = executor.map(process_pair_wrapper, batch_pairs)
            for result in results:
                if result is not None:
                    correlation_table[result[0]] = result[1]

        # Explicitly call garbage collector
        #gc.collect()

    return correlation_table

# Main processing with batch processing
correlation_table = parallel_process_player_pairs(player_mapping)

# Convert the correlation table to a dataframe and write to CSV
flattened_data = []
for players, df in correlation_table.items():
    for index, row in df.iterrows():
        flattened_data.append([players, index] + row.tolist())

columns = ['Player Pair', 'Metric'] + df.columns.tolist()
flattened_df = pd.DataFrame(flattened_data, columns=columns)
flattened_df.set_index(['Player Pair', 'Metric'], inplace=True)

file_path = 'nbaanalytics/player_correlations_spearman.csv'
# flattened_df.to_csv(file_path)
