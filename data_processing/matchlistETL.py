import pandas as pd
import sys
import boto3
import os
import keyconfigs as cfg

#Return a dataframe with all json data from an S3 Bucket
def read_json_from_s3(s3bucket):
    s3 = boto3.resource('s3')
    read_bucket = s3.Bucket(s3bucket)
    data = pd.DataFrame()

    #Append data from all files in bucket
    for obj in read_bucket.objects.all():
        key = obj.key
        body = obj.get()['Body'].read()
        data = data.append(pd.read_json(body), ignore_index=True)

    return(data)

#Write a dataframe to local csv and upload to S3
def write_df_to_csv_s3(dataframe, s3bucket, filename):
    
    s3 = boto3.client('s3')
    filename = filename+'.csv'

    #Write to local csv
    dataframe.drop_duplicates().to_csv(filename, index = False)
    #Upload to S3
    s3.upload_file(filename, s3bucket, filename)
    #Remove local csv
    os.remove(filename)

#Return dataframe for esports table
def esportsETL(matchlist):
    #Unpack nested column
    esports = pd.DataFrame.from_dict(
        matchlist['videogame'].apply(pd.Series)
    )
    #Rename columns
    esports.columns = ['esports_id','esports_name','esports_slug']
    return esports

#Return dataframe for teams table
def teamsETL(matchlist):
    #Unpack nested column
    teams = matchlist['opponents'].apply(pd.Series) \
                                  .stack() \
                                  .reset_index(level=1, drop=True) \
                                  .to_frame('team')
    #Unpack nested column            
    teams = teams['team'].apply(pd.Series)['opponent'] \
                         .apply(pd.Series) \
                         .reset_index(drop=True)
    #Select columns
    teams = teams[['id','name','slug']]
    #Rename columns
    teams.columns = ['team_id','team_name','team_slug']
    return teams

#Return dataframe for leagues table
def leaguesETL(matchlist):
    #Unpack nested column
    leagues = matchlist['league'].apply(pd.Series)
    #Select columns
    leagues = leagues[['id','name','slug']]
    #Rename columns
    leagues.columns = ['league_id', 'league_name', 'league_slug']
    return leagues

#Return dataframe for series table
def seriesETL(matchlist):
    #Unpack nested column
    series = pd.DataFrame(
        matchlist['serie'].apply(pd.Series)
    )

    #Select columns
    series = series[['begin_at', 
                     'end_at',
                     'full_name',
                     'id',
                     'league_id',
                     'name',
                     'prizepool',
                     'season',
                     'slug',
                     'winner_id',
                     'year']]

    #Clean data in winner_id column
    series['winner_id'] = series['winner_id'].fillna(0.0).astype(int)
    #Rename columns
    series.columns = ['begin_at',
                      'end_at',
                      'serie_full_name',
                      'series_id',
                      'league_id',
                      'serie_name',
                      'prizepool',
                      'season',
                      'series_slug',
                      'winner_id',
                      'year']
    return series

#Return dataframe for games table
def gamesETL(matchlist):
    #Unpack nested column, stack games 
    games = matchlist['games'].apply(pd.Series) \
                              .stack() \
                              .reset_index(level=1, drop=True) \
                              .to_frame('game')
    #Unpack nested games column
    games = games['game'].apply(pd.Series)
    #Select columns
    games = games[['begin_at',
                   'end_at',
                   'id',
                   'match_id',
                   'position',
                   'winner',
                   'winner_type']]

    #Extract winner_id and clean data in column
    games['winner'] = games['winner'].apply(pd.Series)['id'] \
                                     .fillna(0.0) \
                                     .astype(int)
    #Rename columns
    games.columns = ['begin_at',
                     'end_at',
                     'game_id',
                     'match_id',
                     'game_number',
                     'winner_id',
                     'winner_type']
    return games

#Return dataframe for matches table
def matchesETL(matchlist):
    #Select columns
    matches = matchlist[['begin_at',
                         'end_at',
                         'id',
                         'league_id',
                         'serie_id',
                         'videogame',
                         'slug',
                         'tournament_id',
                         'winner_id']]

    #Clean data in winner_id column
    matches['winner_id'] = matches['winner_id'].fillna(0.0).astype(int)

    #Extract esports_id from videogame column
    matches['videogame'] = matches['videogame'].transform(
        lambda esport: esport['id'])
    
    #Select columns
    matches = matches[['begin_at', \
                         'end_at', \
                         'id', \
                         'league_id', \
                         'serie_id', \
                         'videogame', \
                         'slug', \
                         'tournament_id', \
                         'winner_id']] 
    #Rename columns
    matches.columns = [['begin_at',
                        'end_at',
                        'match_id',
                        'league_id',
                        'series_id',
                        'esports_id',
                        'slug',
                        'tournament_id',
                        'winner_id']]
    return matches

#Return dataframe for tournaments table
def tournamentsETL(matchlist):
    
    #Unpack nested column
    tournaments = matchlist['tournament'].apply(pd.Series)

    #Select columns
    tournaments = tournaments[['begin_at',
                               'end_at',
                               'id',
                               'league_id',
                               'name',
                               'prizepool',
                               'serie_id',
                               'slug',
                               'winner_id',
                               'winner_type']]
    #Rename columns
    tournaments.columns = [['begin_at',
                            'end_at',
                            'tournament_id',
                            'league_id',
                            'name',
                            'prizepool',
                            'series_id',
                            'slug',
                            'winner_id',
                            'winner_type']]
    return tournaments                       

if __name__ =='__main__':
    s3bucket_raw = cfg.s3['raw']
    #Load raw data from S3
    matchlist = read_json_from_s3(s3bucket_raw)
    
    s3bucket_clean = cfg.s3['clean']
    #Write clean data to S3 
    write_df_to_csv_s3(esportsETL(matchlist), s3bucket_clean, 'esports')
    write_df_to_csv_s3(teamsETL(matchlist), s3bucket_clean, 'teams')
    write_df_to_csv_s3(leaguesETL(matchlist), s3bucket_clean, 'leagues')
    write_df_to_csv_s3(seriesETL(matchlist), s3bucket_clean, 'series')
    write_df_to_csv_s3(gamesETL(matchlist), s3bucket_clean, 'games')
    write_df_to_csv_s3(matchesETL(matchlist), s3bucket_clean, 'matches')
    write_df_to_csv_s3(tournamentsETL(matchlist), s3bucket_clean, 'tournaments')
