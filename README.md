## Python Client for Google BigQuery | Examples
This repository contains some usefull examples of using [python BigQuery client](https://github.com/googleapis/python-bigquery "python BigQuery client"). All of this examples can be found in [this youtube playlist](https://www.youtube.com/playlist?list=PL3JVwFmb_BnRKqcbtl2hHL5GIQOHX-sC5 "this youtube playlist").
**note** code from repository is different then this shown on videos

For more informations about Python BigQuery Client and BigQuery API take a look at:
- [Python BigQuery Client Docs](https://cloud.google.com/python/docs/reference/bigquery/latest/index.html "Python BigQuery Client Docs")
- [BigQuery API Docs](https://cloud.google.com/bigquery/docs/reference/rest "BigQuery API Docs")

##### Adding and deleting table columns | [SOURCE](src/add_delete_columns.py "SOURCE")
```python
python main.py add_delete_columns
```

##### Dry run query | [SOURCE](src/dry_run.py "SOURCE")
```python
python main.py dry_run
```

##### Run SQL queries | [SOURCE](src/quering.py "SOURCE")
```python
python main.py quering
```

##### Loading query result to JSON, CSV, Table | [SOURCE](src/loading_data.py "SOURCE")
```python
python main.py loading_data
```

##### Export data from BigQuery to JSON, CSV, ZIP | [SOURCE](src/file_export.py "SOURCE")
Exporting data from BigQuery table to JSON, CSV, or ZIP in google bucket
```python
python main.py file_export
```

##### Create BigQuery table from JSON file | [SOURCE](src/load_json.py "SOURCE")
```python
python main.py load_json
```

##### Create BigQuery table from CSV file | [SOURCE](src/load_csv.py "SOURCE")
```python
python main.py load_csv
```

##### Connect to Google Sheets | [SOURCE](src/connect_with_google_sheets.py "SOURCE")
```python
python main.py connect_with_google_sheets
```

##### Create View | [SOURCE](src/create_view.py "SOURCE")
```python
python main.py create_view
```

##### List, Create, Delete, Migrate datasets | [SOURCE](src/dataset_crud.py "SOURCE")
```python
python main.py dataset_crud
```

##### Import data from SQL server | [SOURCE](src/sql_import.py "SOURCE")
```python
python main.py sql_import
```