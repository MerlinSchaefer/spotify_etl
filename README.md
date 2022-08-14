# Spotify ETL Project

This repo contains a Spotify ETL project. 

The idea is to query the API regularly and obtain the latest information about songs a user listened to. 
This info should be gathered to build a database of historical music choices.
Additionally I plan to query other sources to obtain information about events, weather, etc. to see if we are really "singing in the rain" or dance in summer :)
The end goal is a dashboard etc. to give more insights into one's music choices over time and in relation to outside influences (if there are any).
Maybe this can even be enriched with a sensible recommendation. 

Additionally I want to use this project to learn and practice some best practices such as CI/CD, type hinting and checking, testing, and data validation through pydantic.

Stack (questionmark means potential but still undecided):

- Python (pandas, spotipy)
- PostgreSQL on RDS 
- AWS Lambda (?)
- AWS EC2 (?) + Airflow (?)



## Spotify Authentication

Authentication is handled by spotipy and oauth2. 
To use `spotipy.oauth2` the `SPOTIPY_CLIENT_ID` and `SPOTIPY_CLIENT_SECRET` environment variables need to be set.