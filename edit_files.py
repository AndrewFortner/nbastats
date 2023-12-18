import pandas as pd

def drop_dates(df):
    # current format for games.csv is YYYY-MM-DD in the GAME_DATE_EST column. Drop all records before 2017
    df['GAME_DATE_EST'] = pd.to_datetime(df['GAME_DATE_EST'])
    df = df[df['GAME_DATE_EST'] > '2017-01-01']
    # Write the new dataframe to a csv file
    df.to_csv('nbadata/games.csv', index=False)

def drop_games(df):
    # Drop all the games from the games_details.csv file that are not in the games.csv file
    games = pd.read_csv('nbadata/games.csv')
    game_ids = games['GAME_ID'].unique()
    df = df[df['GAME_ID'].isin(game_ids)]
    # Write the new dataframe to a csv file
    df.to_csv('nbadata/games_details.csv', index=False)


def add_players():
    # Make a mapping from the player ID to the player name from games_details.csv. Write it to players.csv
    # Make sure it is in the same format as the current players.csv file
    # PLAYER_NAME,TEAM_ID,PLAYER_ID,SEASON (set SEASON to 0)
    games_details = pd.read_csv('nbadata/games_details.csv')
    players = games_details[['PLAYER_NAME', 'PLAYER_ID', 'TEAM_ID']]
    players = players.drop_duplicates()
    players['SEASON'] = 0
    players.to_csv('nbadata/players.csv', index=False)
    
def remove_duplate_ids():
    # Remove duplicate player IDs from players.csv
    players = pd.read_csv('nbadata/players.csv')
    players = players.drop_duplicates(subset=['PLAYER_ID'])
    players.to_csv('nbadata/players.csv', index=False)
