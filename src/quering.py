from google.cloud import bigquery
from google.oauth2 import service_account
from dataclasses import dataclass, field


@dataclass
class GClientFactory:
    credentials_path: str = ""
    credentials: service_account.Credentials | None = field(default=None, init=False)

    def __post_init__(self):
        self.credentials = self.__credentials

    @property
    def __credentials(self) -> service_account.Credentials | None:
        if not self.credentials_path:
            return None

        return service_account.Credentials.from_service_account_file(
            self.credentials_path
        )

    def get_client(self) -> bigquery.Client:
        """Return instance of bigquery cliet"""

        return bigquery.Client(credentials=self.credentials)


@dataclass
class Row:
    items: dict = field(repr=False)

    def __post_init__(self):
        for key, value in self.items.items():
            setattr(self, key, value)

    def __repr__(self) -> str:
        rows = "".join([f"{x}={y}, " for x, y in self.items.items()])
        return f"Row({rows})"


def query():
    client = GClientFactory().get_client()

    # Perform a query.
    QUERY = """
        SELECT region_name, week, score FROM `bigquery-public-data.google_trends.international_top_rising_terms`
        ORDER BY score DESC
        LIMIT 20
    """

    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    for row in rows:
        r = Row(items=dict(row))
        print(r)


def main():
    query()
