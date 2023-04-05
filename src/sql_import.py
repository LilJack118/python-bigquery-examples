from google.cloud import bigquery
from dataclasses import dataclass, field
import pypyodbc as odbc  # to communicate with SQL Server
import pandas as pd
import time


@dataclass
class SQLServer:
    server_name: str
    database_name: str
    _conn: None | odbc.Connection = field(default=None, init=False)
    _cursor: None | odbc.Cursor = field(default=None, init=False)

    def __setattr__(self, name, value) -> None:
        # if setting new connection and cursor is not None, set cursor to None
        if name == "_conn" and self._cursor:
            self._cursor = None

        super().__setattr__(name, value)

    @classmethod
    @property
    def DRIVER_NAME(cls) -> str:
        return "SQL Server"

    @property
    def conn(self) -> odbc.Connection:
        if not self.check_connection():
            raise Exception("Connection is absent")

        return self._conn

    @property
    def _connection_string(self) -> str:
        return f"""
            DRIVER={{{self.DRIVER_NAME}}};
            SERVER={self.server_name};
            DATABASE={self.database_name};
            Trust_Connection=yes;
        """

    @property
    def cursor(self) -> odbc.Cursor:
        if not self._cursor:
            self._cursor = self.conn.cursor()
        return self._cursor

    def connect_to_sql_server(self) -> odbc.Connection:
        self._conn = odbc.connect(self._connection_string)
        return self._conn

    def query(self, query: str) -> tuple:
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        columns = [col[0] for col in self.cursor.description]
        return columns, data


@dataclass
class SQLImporter:
    server_name: str
    database_name: str
    table_id: str
    client: bigquery.Client() | None = field(default_factory=lambda: bigquery.Client())
    _sql_server_instance: None | SQLServer = field(default=None, init=False)
    _data: None | pd.DataFrame = field(default=None, init=False)

    @property
    def data(self) -> None | pd.DataFrame:
        if not self._data:
            raise Exception("Data is not queried")
        return self._data

    @property
    def sql_server_instance(self) -> SQLServer:
        if not self._sql_server_instance:
            raise Exception("SQL Server instance not set")

        return self._sql_server_instance

    @property
    def job_config(self) -> bigquery.LoadJobConfig:
        return bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
        )

    def connect(self) -> None:
        sql_server_instance = SQLServer(self.server_name, self.database_name)
        sql_server_instance.connect_to_sql_server()
        # if connection is successful, set instance attribute
        self._sql_server_instance = sql_server_instance

    def query_data(self, query: str) -> None:
        columns, data = self.sql_server_instance.query(query)
        self._data = pd.DataFrame(data, columns=columns)

    def load_data(self) -> None:
        job = self.client.load_table_from_dataframe(
            self.data, self.table_id, job_config=self.job_config
        )
        while job.state != "Done":
            time.sleep(2)
            job.reload()
        print(job.result())


def main():
    importer = SQLImporter(
        server_name="YOUR_SERVER_NAME",
        database_name="YOUR_DATABASE_NAME",
        table_id="bigq_dataset.table_id",
    )
    # connect to SQL Server
    importer.connect()
    QUERY = "SELECT * FROM table_name"
    # query data
    importer.query_data(QUERY)
    # load data to BigQuery
    importer.load_data()
