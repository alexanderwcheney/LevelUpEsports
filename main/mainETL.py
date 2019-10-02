

import pandas as pd


def esportsETL(matchlist):
    esports = pd.DataFrame.from_dict(
        matchlist['videogame'].apply(pd.Series)
    )

    esports.columns = ['esports_id','esports_name','esports_slug']

    esports.to_csv('esports.csv')


def teamsETL(matchlist):
    teams = matchlist['opponents'].apply(pd.Series) \
                                  .stack() \
                                  .reset_index(level=1, drop=True) \
                                  .to_frame('team')
                
    teams = teams['team'].apply(pd.Series)['opponent'] \
                         .apply(pd.Series) \
                         .reset_index(drop=True)
            
    teams = teams[['id','name','slug']]

    teams.columns = ['team_id','team_name','team_slug']

    teams.to_csv('teams.csv')




def leaguesETL(matchlist):
    leagues = matchlist['league'].apply(pd.Series)

    leagues = leagues[['id','name','slug']]

    leagues.columns = ['league_id', 'league_name', 'league_slug']

    leagues.to_csv('leagues.csv')

def seriesETL(matchlist):
    series = pd.DataFrame(
        matchlist['serie'].apply(pd.Series)
    )

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

    series['winner_id'] = series['winner_id'].fillna(0.0).astype(int)

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
    series.to_csv('series.csv')

def gamesETL(matchlist):
    games = matchlist['games'].apply(pd.Series) \
                              .stack() \
                              .reset_index(level=1, drop=True) \
                              .to_frame('game')
                
    games = games['game'].apply(pd.Series)

    games = games[['begin_at',
                   'end_at',
                   'id',
                   'match_id',
                   'position',
                   'winner',
                   'winner_type']]

    games['winner'] = games['winner'].apply(pd.Series)['id'] \
                                     .fillna(0.0) \
                                     .astype(int)

    games.columns = ['begin_at',
                     'end_at',
                     'game_id',
                     'match_id',
                     'game_number',
                     'winner_id',
                     'winner_type']
    games.to_csv('games.csv')

def matchesETL(matchlist):
    matches = matchlist[['begin_at',
                         'end_at',
                         'id',
                         'league_id',
                         'serie_id',
                         'slug',
                         'tournament_id',
                         'winner_id',
                         'opponents']]

    matches['winner_id'] = matches['winner_id'].fillna(0.0).astype(int)

    matches[['team1_id','team2_id']] = matches['opponents'].apply(
        lambda participants:
        pd.Series(
            [participant['opponent']['id'] for participant in participants] 
        )
    )
    matches.to_csv('matches.csv')


def tournamentsETL(matchlist):
    tournaments = matchlist['tournament'].apply(pd.Series)

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
    tournaments.to_csv('tournaments.csv')

if __name__ == "__main__":
    matchlist = pd.read_json("CSGOmatchesPAST1.json")
    esportsETL(matchlist)
    teamsETL(matchlist)
    leaguesETL(matchlist)
    seriesETL(matchlist)
    gamesETL(matchlist)
    matchesETL(matchlist)
    tournamentsETL(matchlist)