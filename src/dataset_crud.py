from google.cloud import bigquery, bigquery_datatransfer
from dataclasses import dataclass


@dataclass
class BigQueryDatasetManager:
    client: bigquery.Client | None = None

    def __post_init__(self):
        self.client = self.client or bigquery.Client()

    def delete_dataset(self, dataset_id: str) -> None:
        self.client.delete_dataset(dataset_id, not_found_ok=True)

    def create_dataset(self, dataset_id: str, **kwargs) -> None:
        dataset = bigquery.Dataset(dataset_id)
        for k, v in kwargs.items():
            setattr(dataset, k, v)

        dataset = self.client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {self.client.project}.{dataset_id}")
        return dataset

    def list_datasets(self) -> list[str]:
        return [d.dataset_id for d in self.client.list_datasets()]

    def copy_dataset(
        self,
        src_project_id: str,
        src_dataset_id: str,
        display_name: str,
        dest_project_id: str,
        dest_dataset_id: str,
    ):
        transfer_client = bigquery_datatransfer.DataTransferServiceClient()
        transfer_config = bigquery_datatransfer.TransferConfig(
            destination_dataset_id=dest_dataset_id,
            display_name=display_name,
            data_source_id="cross_region_copy",
            params={
                "source_project_id": src_project_id,
                "source_dataset_id": src_dataset_id,
            },
        )
        transfer_config = transfer_client.create_transfer_config(
            parent=transfer_client.common_project_path(dest_project_id),
            transfer_config=transfer_config,
        )
        print(f"Created transfer config {transfer_config.name}")


def main():
    mg = BigQueryDatasetManager()

    # list
    print(mg.list_datasets())

    # create
    mg.create_dataset("bigq-etl.test_dataset", description="test dataset")

    # copy
    mg.copy_dataset(
        src_project_id="bigq-etl",
        src_dataset_id="test_dataset",
        display_name="Test dataset copy",
        dest_project_id="bigq-etl",
        dest_dataset_id="test_dataset_cp",
    )

    # delete
    mg.delete_dataset("test_dataset_cp")
    mg.delete_dataset("test_dataset")
