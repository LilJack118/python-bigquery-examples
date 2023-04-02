import time
from google.cloud import bigquery
from dataclasses import dataclass


@dataclass
class DryQuery:
    query: str

    def __post_init__(self):
        self.client = bigquery.Client()

    def __format_dry_job(self, job) -> None:
        print(f"Total MB processed: {job.total_bytes_processed/1_000_000}MB")
        print(f"Total MB billed: {job.total_bytes_billed/1_000_000}MB")

    def run(self, **kwargs):
        job_conf = bigquery.QueryJobConfig(
            dry_run=True, use_query_cache=False, **kwargs
        )
        job = self.client.query(self.query, job_config=job_conf)
        self.__format_dry_job(job)


def main():
    QUERY = """
        SELECT region_name, week, score FROM `bigquery-public-data.google_trends.international_top_rising_terms`
        ORDER BY score DESC
        LIMIT 20
    """

    dry_query = DryQuery(query=QUERY)
    dry_query.run()
