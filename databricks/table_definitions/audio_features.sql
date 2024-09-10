CREATE TABLE IF NOT EXISTS audio_features (
    id STRING PRIMARY KEY,
    danceability DOUBLE, 
    energy DOUBLE, 
    key INT, 
    loudness DOUBLE, 
    mode INT, 
    speechiness DOUBLE,
    acousticness DOUBLE, 
    instrumentalness DOUBLE, 
    liveness DOUBLE, 
    valence DOUBLE, 
    tempo DOUBLE,
    analysis_url STRING, 
    time_signature INT
);
