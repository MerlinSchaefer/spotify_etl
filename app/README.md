# ETL App Code

This folder contains the code for the various pipeline steps.
Some credentials are required and need to be set up.
One way is to set  up environment variables or an `.env` file and running the following command `export $(cat app/.env | xargs)`.
The other and more convenient option is to set up a `config.ini` file and use the `configparser` library, which I have done for the main application.
