# Air Quality Data Pipeline

### Download the daily data files from the European repository

`cargo run`

### Importing the data after download

Install `csvkit` in order to get `csvstack`: `brew install csvkit`

Use `csvstack` to concatenate the downloaded files:

    csvstack data/<YYYY>/<M>/<DD>/**/*.csv -e iso-8859-1 > output.csv       
Import concatenated file into mongodb:

    mongoimport --type csv -d test -c airquality --headerline output.csv               
