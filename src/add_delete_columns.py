from google.cloud import bigquery
from dataclasses import dataclass, field
import time


@dataclass
class BigQueryDataset:
    dataset_id: str
    client: bigquery.Client = field(default_factory=lambda: bigquery.Client())

    @property
    def dataset_ref(self) -> bigquery.DatasetReference:
        return bigquery.DatasetReference(self.client.project, self.dataset_id)

    def get_table(self, table_id: str) -> bigquery.Table:
        print("Getting table")
        table_ref = bigquery.TableReference(self.dataset_ref, table_id)
        return self.client.get_table(table_ref)

    def append_cols(self, table_id: str, cols: list[bigquery.SchemaField]) -> None:
        table = self.get_table(table_id)
        og_schema = table.schema
        new_schema = og_schema[:]
        new_schema.extend(cols)
        table.schema = new_schema
        print(f"Adding {', '.join(c.name for c in cols)}")
        self.client.update_table(table, ["schema"])

    def drop_cols(self, table_id: str, cols: list[str]) -> None:
        print(f"Droping {', '.join(c for c in cols)}")
        query = f"ALTER TABLE {self.dataset_id}.{table_id} {','.join(f'DROP COLUMN IF EXISTS {c}' for c in cols)};"
        job = self.client.query(query)
        while job.state != "DONE":
            time.sleep(2)
            job.reload()
        print(job.result())


def main():
    dt = BigQueryDataset("nasa")
    cols = [bigquery.SchemaField("new_col", "STRING", mode="NULLABLE")]
    dt.append_cols("near_earth_commets", cols)
    time.sleep(3)
    dt.drop_cols("near_earth_commets", [c.name for c in cols])
