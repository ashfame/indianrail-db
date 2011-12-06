#!/usr/bin/env bash

# Dump json
python export.py

cd data
tables="trains stations schedule"

# Copy SQLite
cp ../prod.sqlite3 indian_railways.sqlite3


# Cleanup json
for table in $tables
do
    cat $table.json | python -mjson.tool > a.tmp
    mv a.tmp $table.json
done

# Dump SQL
for table in $tables
do
    sqlite3 indian_railways.sqlite3 ".dump $table" > $table.sql
done

# Dump csv
for table in $tables
do
sqlite3 indian_railways.sqlite3 << _EO_CSV
.headers on
.mode csv
.separator ,
.output $table.csv
select * from $table;
_EO_CSV
done

# Convert csv to xml
for table in $tables
do
    csv2xml <$table.csv >$table.xml
done


# Zip
rm *.zip
zip indian_railways.sqlite3.zip indian_railways.sqlite3
zip indian_railways.sql.zip *.sql
zip indian_railways.json.zip *.json
zip indian_railways.xml.zip *.xml
zip indian_railways.csv.zip *.csv
