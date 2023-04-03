from google.cloud import bigquery
from dataclasses import dataclass
import time, json, tempfile


@dataclass
class BigQueryJsonLoader:
    file_path: str
    table_id: str

    def __post_init__(self):
        self.client = bigquery.Client()

    @property
    def json_data(self) -> bytes:
        data = json.load(open(self.file_path, "r"))
        return "\n".join([json.dumps(row) for row in data]).encode("utf-8")

    @property
    def job_config(self) -> bigquery.LoadJobConfig:
        return bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_TRUNCATE",
        )

    def load(self):
        with tempfile.NamedTemporaryFile(mode="w+b") as tmp:
            tmp.write(self.json_data)
            tmp.seek(0)
            job = self.client.load_table_from_file(
                tmp, self.table_id, job_config=self.job_config
            )

        while job.state != "DONE":
            time.sleep(2)
            job.reload()

        print(job.result())


def main():
    TABLE_ID = "bigq-etl.nasa.earth_meteorite_landings"
    FILE_PATH = (
        "/Users/kuba/Desktop/python_bigquery/files/earth_meteorite_landings.json"
    )
    loader = BigQueryJsonLoader(FILE_PATH, TABLE_ID)
    loader.load()
