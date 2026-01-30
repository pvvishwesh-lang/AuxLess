from fastapi import FastAPI,Header,HTTPException,Depends
import pandas as pd

app=FastAPI()

@app.get('/')
def home():
    return {'Api Running'}


df=pd.read_csv('/Users/vishweshpv/Coding/Project/My_Playlist.csv')
df['Genres'].fillna('Unknown',inplace=True)


def authentication(authorization:str=Header(None)):
    if not authorization:
        raise HTTPException(status_code=401,detail='Unauthorized')
    return authorization.replace('Bearer','')

@app.get('/v1/me/top/tracks')
def get_top_tracks(token:str=Depends(authentication)):
    user_id=token
    user_df=df[df['Added By']==user_id] if 'Added By' in df else df
    tracks=[]
    for _,row in user_df.iterrows():
        tracks.append({
            'id':f"mock_{hash(row['Track Name'])}",
            'name':row['Track Name'],
            'duration_ms':int(row['Duration (ms)']),
            'explicit':bool(row['Explicit']),
            'popularity':int(row['Popularity']),
            'artists':[
                {'name':a.strip()}
                for a in row['Artist Name(s)'].split(',')
            ],
            'genres':row['Genres'].split(','),
            'audio_features':{
                'danceability':row['Danceability'],
                'energy':row['Energy'],
                'valence':row['Valence'],
                'tempo':row['Tempo']
            }
        })
    return {
        'items':tracks,
        'total':len(tracks)
    }

@app.get('/v1/audio-features')
def audio_features(track_ids:str):
    ids=track_ids.split(',')
    features=[]
    for track_id in ids:
        row=df.iloc[abs(hash(track_id))%len(df)]
        features.append({
            'id':track_id,
            'danceability':row['Danceability'],
            'energy':row['Energy'],
            'key':row['Key'],
            'loudness':row['Loudness'],
            'mode':row['Mode'],
            'speechiness':row['Speechiness'],
            'acousticness':row['Acousticness'],
            'instrumentalness':row['Instrumentalness'],
            'liveness':row['Liveness'],
            'valence':row['Valence'],
            'tempo':row['Tempo'],
            'time_signature':row['Time Signature']
        })
    return {'audio_features':features}

