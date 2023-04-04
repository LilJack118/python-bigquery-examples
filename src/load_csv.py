from google.cloud import bigquery
from dataclasses import dataclass
import time, os


@dataclass
class BigQueryCsvLoader:
    file_path: str

    def __post_init__(self):
        self._validate()

        self.client = bigquery.Client()

    def _validate(self) -> None:
        # validate file_path
        if not self.file_path.endswith(".csv"):
            raise Exception("File must be a csv file.")

        if not os.path.exists(self.file_path):
            raise Exception("File does not exist.")

    @property
    def job_config(self) -> bigquery.LoadJobConfig:
        return bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition="WRITE_TRUNCATE",
        )

    def load(self, table_id: str) -> None:
        with open(self.file_path, "rb") as f:
            job = self.client.load_table_from_file(
                f, table_id, job_config=self.job_config
            )

        while job.state != "DONE":
            time.sleep(2)
            job.reload()

        print(job.result())


full_path = lambda relative_path: os.path.abspath(relative_path)


def main():
    FILE_PATH = "files/near_earth_commets.csv"
    TABLE_ID = "bigq-etl.nasa.near_earth_commets"
    loader = BigQueryCsvLoader(file_path=full_path(FILE_PATH))
    loader.load(TABLE_ID)
