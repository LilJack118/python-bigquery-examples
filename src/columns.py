from google.cloud import bigquery


def main():
    client = bigquery.Client()
    dataset_ref = bigquery.DatasetReference("bigquery-public-data", "google_trends")
    table_ref = bigquery.TableReference(dataset_ref, "international_top_rising_terms")

    table = client.get_table(table_ref)

    og_schema = table.schema

    new_schema = og_schema[:]
    new_schema.append(bigquery.SchemaField("new_col", "STRING", mode="NULLABLE"))

    table.schema = new_schema

    # update schema
    client.update_table(table, ["schema"])
