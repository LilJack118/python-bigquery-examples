from google.cloud import bigquery
import time
import dataclasses


@dataclasses.dataclass
class BigQueryLoader:
    query: str
    project_id: str
    dataset_id: str
    table_id: str

    def __post_init__(self):
        self.client = bigquery.Client()

    @property
    def dataset_ref(self):
        return self.client.dataset(self.dataset_id, self.project_id)

    @property
    def destination_dataset(self) -> bigquery.Dataset:
        return self.client.get_dataset(self.dataset_ref)

    @property
    def destination_tb(self) -> bigquery.Table:
        return self.dataset_ref.table(self.table_id)

    @property
    def query_job_conf(self) -> bigquery.QueryJobConfig:
        return bigquery.QueryJobConfig(destination=self.destination_tb)

    def to_csv(self, df):
        df.to_csv("test.csv")

    def to_json(self, df):
        df.T.to_json("test.json")

    def load(self, load_to: list[str]):
        if "table" in load_to:
            load_to.pop("table")
            location, job_config = (
                self.destination_dataset.location,
                self.query_job_conf,
            )
        else:
            location, job_config = None, None

        query_job = self.client.query(
            self.query,
            location=location,
            job_config=job_config,
        )

        while query_job.state != "DONE":
            time.sleep(3)
            query_job.reload()

        df = query_job.to_dataframe()
        for l in load_to:
            func = getattr(self, f"to_{l}")
            func(df)

        print("Query saved to")


def main():
    QUERY = """
        SELECT region_name, week, score FROM `bigquery-public-data.google_trends.international_top_rising_terms`
        ORDER BY score DESC
        LIMIT 20
    """
    bigQ_loader = BigQueryLoader(
        query=QUERY, project_id="bigq-etl", dataset_id="Staging", table_id="test"
    )
    bigQ_loader.load(load_to=["csv", "json"])
