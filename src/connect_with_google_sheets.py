from google.cloud import bigquery
from dataclasses import dataclass
import google.auth
import google.oauth2


@dataclass
class BigQueryConnectedGoogleSheet:
    sheet_url: str
    dataset_id: str
    table_id: str

    def __post_init__(self):
        credentials, project = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery",
            ],
        )

        self.client = bigquery.Client(credentials=credentials, project=project)

    @property
    def dataset(self) -> bigquery.Dataset:
        return self.client.get_dataset(self.dataset_id)

    @property
    def external_config(self) -> bigquery.ExternalConfig:
        external_config = bigquery.ExternalConfig("GOOGLE_SHEETS")
        external_config.autodetect = True
        external_config.source_uris = [self.sheet_url]
        return external_config

    @property
    def table(self) -> bigquery.Table:
        table = bigquery.Table(self.dataset.table(self.table_id))
        table.external_data_configuration = self.external_config
        return table

    def create_table(self) -> None:
        """Create table connected to Google Sheets"""

        table = self.client.create_table(self.table)


def main():
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1MxvWi-4AbgXKyjzMc11u3yXcFEFTTWVPr_PJ6o5DzzI/edit#gid=113404796"
    DATASET_ID = "bigq-etl.nasa"
    TABLE_ID = "nasa_facilities"

    sheet = BigQueryConnectedGoogleSheet(
        sheet_url=SHEET_URL, dataset_id=DATASET_ID, table_id=TABLE_ID
    )
    sheet.create_table()
