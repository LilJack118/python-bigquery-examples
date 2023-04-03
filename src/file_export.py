from google.cloud import bigquery
from dataclasses import dataclass, field


@dataclass
class BigQueryExporter:
    dataset_id: str
    table_id: str
    bucket_name: str
    project_id: str | None = field(default=None)
    # init exluded attributes
    client: None = field(init=False, default=None)

    def __post_init__(self):
        self.client = bigquery.Client()
        if self.project_id is None:
            self.project_id = self.client.project

    @property
    def table_ref(self) -> bigquery.TableReference:
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
        return dataset_ref.table(self.table_id)

    @property
    def job_config(self) -> bigquery.QueryJobConfig:
        job_config = bigquery.job.ExtractJobConfig()
        job_config.destination_format = (
            bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
        )
        return job_config

    def export_file(self, destination_uri: str) -> None:
        extract_job = self.client.extract_table(
            self.table_ref,
            destination_uri,
            job_config=self.job_config,
            # location must match that of the source table.
            location="US",
        )
        print(extract_job.result())

    def export_to_json(self, destination_uri: str) -> None:
        self.export_file(destination_uri)

    def export_to_csv(self, destination_uri: str) -> None:
        self.export_file(destination_uri)

    def export(self, file_name: str, formats: None | list[str] = None) -> None:
        if formats is None:
            formats = []

        for f in formats:
            destination_uri = f"gs://{self.bucket_name}/{file_name}.{f}"
            export_func = getattr(self, f"export_to_{f}")
            export_func(destination_uri)


def main():
    exporter = BigQueryExporter(
        project_id="bigquery-public-data",
        dataset_id="austin_bikeshare",
        table_id="bikeshare_stations",
        bucket_name="bigquery-bucket-123123",
    )
    exporter.export(file_name="bigquery-public-data", formats=["json"])
