# meltano-and-spark-practice-with-investment-data
A project for practicing meltano and spark

## EL
Add below lines to .env file for google sheets authentication
```bash
TAP_GOOGLE_SHEETS_CLIENT_SECRET='secret'
TAP_GOOGLE_SHEETS_REFRESH_TOKEN='refresh_token'
```
Run extraction of all parities and load it as parquet file. Files will be created at output directory
```bash
meltano run tap-turkish_lira_parities target-parquet
```
Run extraction of google sheets data which contains buying amounts,dates and prices for each investment instrument
```bash
meltano run --full-refresh tap-google-sheets target-parquet
```

## T
Run notebook `Investment Transformations.ipynb`