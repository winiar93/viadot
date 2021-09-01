import prefect
from prefect import Flow, task, Task
from typing import Dict, List, Any
import pendulum
import os

from abc import abstractmethod

from viadot.task_utils import (
    add_ingestion_metadata,
    get_df_dtypes,
    dict_to_json,
    df_to_parquet,
)
from viadot.tasks import RunGreatExpectationsValidation


class BaseExtract(Flow):
    def __init__(
        self,
        name: str,
        download_task_kwargs,
        expectation_suite: dict = None,
        evaluation_parameters: dict = None,
        keep_validation_output: bool = False,
        local_file_path: str = None,
        adls_dir_path: str = None,
        overwrite_adls: bool = True,
        if_empty: str = "warn",
        if_exists: str = "replace",
        adls_sp_credentials_secret: str = None,
        max_download_retries: int = 5,
        parallel: bool = True,
        tags: List[str] = ["extract"],
        vault_name: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):

        self.download_task_kwargs = download_task_kwargs

        # RunGreatExpectationsValidation
        self.expectation_suite = expectation_suite
        self.expectations_path = "/home/viadot/tmp/expectations"
        self.expectation_suite_name = expectation_suite["expectation_suite_name"]
        self.evaluation_parameters = evaluation_parameters
        self.keep_validation_output = keep_validation_output

        # AzureDataLakeUpload
        self.local_file_path = local_file_path or self.slugify(name) + ".parquet"
        self.local_json_path = self.slugify(name) + ".json"
        self.now = str(pendulum.now("utc"))
        self.adls_dir_path = adls_dir_path
        self.adls_file_path = os.path.join(adls_dir_path, self.now + ".parquet")
        self.adls_schema_file_dir_file = os.path.join(
            adls_dir_path, "schema", self.now + ".json"
        )
        self.overwrite_adls = overwrite_adls
        self.if_empty = if_empty
        self.adls_sp_credentials_secret = adls_sp_credentials_secret

        # Global
        self.max_download_retries = max_download_retries
        self.parallel = parallel
        self.tags = tags
        self.vault_name = vault_name
        self.if_exists = if_exists

        super().__init__(name=name, *args, **kwargs)

        self.gen_flow()

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    @property
    def download_data_task(self):
        raise NotImplementedError()

    @property
    def upload_data_task(self):
        raise NotImplementedError()

    @property
    def upload_data_schema_task(self):
        raise NotImplementedError()

    @property
    def add_ingestion_metadata_task(self):
        return add_ingestion_metadata

    @property
    def validation_task(self):
        return RunGreatExpectationsValidation()

    @property
    def dict_to_json_task(self):
        return dict_to_json

    @property
    def get_df_dtypes_task(self):
        return get_df_dtypes

    @property
    def df_to_parquet_task(self):
        return df_to_parquet

    def gen_flow(self) -> Flow:
        df = self.download_data_task(**self.download_task_kwargs, flow=self)
        write_json = dict_to_json.bind(
            dict_=self.expectation_suite,
            path=os.path.join(
                self.expectations_path, self.expectation_suite_name + ".json"
            ),
            flow=self,
        )
        validation = self.validation_task.bind(
            df=df,
            expectations_path=self.expectations_path,
            expectation_suite_name=self.expectation_suite_name,
            evaluation_parameters=self.evaluation_parameters,
            keep_output=self.keep_validation_output,
            flow=self,
        )
        df_with_metadata = self.add_ingestion_metadata_task.bind(df, flow=self)
        to_parquet = self.df_to_parquet_task.bind(
            df=df_with_metadata,
            path=self.local_file_path,
            if_exists=self.if_exists,
            flow=self,
        )
