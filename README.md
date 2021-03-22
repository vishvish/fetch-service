# Air Quality Data Pipeline

Imports European Air Quality data from the service here: https://discomap.eea.europa.eu/map/fme/AirQualityUTDExport.htm

Files are recreated every 30 minutes and contain 48 hours of data.

### Download the daily data files from the European repository

`cargo run`

### Importing the data after download

Install `csvkit` in order to get `csvstack`: `brew install csvkit`

Use `csvstack` to concatenate the downloaded files:

    csvstack data/<YYYY>/<M>/<DD>/**/*.csv -e iso-8859-1 > output.csv       
Import concatenated file into mongodb:

    mongoimport --type csv -d test -c airquality --headerline output.csv               
