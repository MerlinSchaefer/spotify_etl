# Spotify ETL Project

This repo contains a Spotify ETL project.

The idea is to query the Spotify API regularly and obtain the latest information about songs a user listened to.
This info is gathered to build a database of historical music choices.
Additionally I plan to query other sources to obtain information about events, weather, etc. to see if we are really "singing in the rain" or dance in summer :)
The end goal is a dashboard, automatic playlist generation etc. to give more insights into one's music choices over time and in relation to outside influences (if there are any).
Maybe this can even be enriched with a sensible recommendation.

Additionally I want to use this project to learn and practice some best practices such as CI/CD, type hinting and checking, testing, and data validation through pydantic.

Tech Stack (question mark means potential but still undecided):

- Python (pandas, spotipy, mypy, pydantic, psycopg2, boto3, black)
- PostgreSQL on RDS
- AWS EC2 + cronjob (Airflow could technically be used, dag is functional, complications of setup on EC2 didn't warrant the effort. A simple cronjob works for the hourly data ingest.) 

# V2.0 Databricks

- after my free AWS tier ran out the project was stopped
- decided to relaunch on Azure
- starting with a setup on Databricks as this will be quickest and easiest for me
- once I have it running may add a platform independent version again


## Spotify Authentication

Authentication is handled by spotipy and oauth2.
To use `spotipy.oauth2` the `SPOTIPY_CLIENT_ID` and `SPOTIPY_CLIENT_SECRET` environment variables need to be set. 
In order to obtain a set of credentials you need to register with the [Spotify Developer Service](https://developer.spotify.com/).

