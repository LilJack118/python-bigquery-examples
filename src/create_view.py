from google.cloud import bigquery
from dataclasses import dataclass


@dataclass
class BigQueryViewFactory:
    query: str
    view_id: str

    def __post_init__(self):
        self.client = bigquery.Client()

    @property
    def view(self) -> bigquery.Table:
        view = bigquery.Table(self.view_id)
        view.view_query = self.query
        return view

    def create(self) -> None:
        self.client.create_table(self.view)


def main():
    QUERY = """
        SELECT object, epoch__tdb_ FROM `bigq-etl.nasa.near_earth_commets`
        ORDER BY epoch__tdb_ DESC
        LIMIT 10
    """
    VIEW_ID = "bigq-etl.nasa.near_earth_commets_view"
    view_factory = BigQueryViewFactory(query=QUERY, view_id=VIEW_ID)
    view_factory.create()
