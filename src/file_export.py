from google.cloud import bigquery
from dataclasses import dataclass, field
from typing import Iterator


@dataclass
class BigQueryExporter:
    dataset_id: str
    table_id: str
    bucket_name: str
    project_id: str | None = field(default=None)
    # init exluded attributes
    client: bigquery.Client | None = field(init=False, default=None)
    comporess: bool = field(init=False, default=False)

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
        if self.comporess:
            job_config.compression = bigquery.Compression.GZIP
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

    def export(
        self, file_name: str, formats: None | list[str] = None, compress=False
    ) -> None:
        if formats is None:
            formats = []

        self.comporess = compress

        for save_f, norm_f in self._normalized_formats(formats):
            destination_uri = f"gs://{self.bucket_name}/{file_name}.{save_f}"
            export_func = getattr(self, f"export_to_{norm_f}")
            export_func(destination_uri)

    def _normalized_formats(self, formats: list[str]) -> Iterator[str]:
        comporess_ext = "" if not self.comporess else ".gz"
        for f in formats:
            f = f.split(".", 1)[0]
            yield f"{f}{comporess_ext}", f


def main():
    exporter = BigQueryExporter(
        project_id="bigquery-public-data",
        dataset_id="austin_bikeshare",
        table_id="bikeshare_stations",
        bucket_name="bigquery-bucket-123123",
    )
    exporter.export(file_name="bigquery-public-data", formats=["json"], compress=True)
