import copy
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Literal

import pandas as pd
import prefect
from prefect import task
from prefect.storage import Git
from prefect.utilities import logging
from visions.functional import infer_type
from visions.typesets.complete_set import CompleteSet
from viadot.sources import AzureSQL


logger = logging.get_logger()
METADATA_COLUMNS = {"_viadot_downloaded_at_utc": "DATETIME"}


@task
def add_ingestion_metadata_task(
    df: pd.DataFrame,
):
    """Add ingestion metadata columns, eg. data download date

    Args:
        df (pd.DataFrame): input DataFrame.
    """
    df2 = df.copy(deep=True)
    df2["_viadot_downloaded_at_utc"] = datetime.now(timezone.utc).replace(microsecond=0)
    return df2


@task
def get_latest_timestamp_file_path(files: List[str]) -> str:
    """
    Return the name of the latest file in a given data lake directory,
    given a list of paths in that directory. Such list can be obtained using the
    `AzureDataLakeList` task. This task is useful for working with immutable data lakes as
    the data is often written in the format /path/table_name/TIMESTAMP.parquet.
    """

    logger = prefect.context.get("logger")

    extract_fname = (
        lambda f: os.path.basename(f).replace(".csv", "").replace(".parquet", "")
    )
    file_names = [extract_fname(file) for file in files]
    latest_file_name = max(file_names, key=lambda d: datetime.fromisoformat(d))
    latest_file = files[file_names.index(latest_file_name)]

    logger.debug(f"Latest file: {latest_file}")

    return latest_file


@task
def dtypes_to_json_task(dtypes_dict, local_json_path: str):
    with open(local_json_path, "w") as fp:
        json.dump(dtypes_dict, fp)


@task
def chunk_df(df: pd.DataFrame, size: int = 10_000) -> List[pd.DataFrame]:
    """
    Creates pandas Dataframes list of chunks with a given size.
    Args:
        df (pd.DataFrame): Input pandas DataFrame.
        size (int, optional): Size of a chunk. Defaults to 10000.
    """
    n_rows = df.shape[0]
    chunks = [df[i : i + size] for i in range(0, n_rows, size)]
    return chunks


@task
def df_get_data_types_task(df: pd.DataFrame) -> dict:
    """
    Returns dictionary containing datatypes of pandas DataFrame columns.
    Args:
        df (pd.DataFrame): Input pandas DataFrame.
    """
    typeset = CompleteSet()
    dtypes = infer_type(df, typeset)
    dtypes_dict = {k: str(v) for k, v in dtypes.items()}
    return dtypes_dict


@task
def get_sql_dtypes_from_df(df: pd.DataFrame) -> dict:
    """Obtain SQL data types from a pandas DataFrame"""
    typeset = CompleteSet()
    dtypes = infer_type(df, typeset)
    dtypes_dict = {k: str(v) for k, v in dtypes.items()}
    dict_mapping = {
        "Float": "REAL",
        "Image": None,
        "Categorical": "VARCHAR(500)",
        "Time": "TIME",
        "Boolean": "BIT",
        "DateTime": "DATETIMEOFFSET",  # DATETIMEOFFSET is the only timezone-aware dtype in TSQL
        "Object": "VARCHAR(500)",
        "EmailAddress": "VARCHAR(50)",
        "File": None,
        "Geometry": "GEOMETRY",
        "Ordinal": "VARCHAR(500)",
        "Integer": "INT",
        "Generic": "VARCHAR(500)",
        "UUID": "UNIQUEIDENTIFIER",
        "Complex": None,
        "Date": "DATE",
        "String": "VARCHAR(500)",
        "IPAddress": "VARCHAR(39)",
        "Path": "VARCHAR(500)",
        "TimeDelta": "VARCHAR(20)",  # datetime.datetime.timedelta; eg. '1 days 11:00:00'
        "URL": "VARCHAR(500)",
        "Count": "INT",
    }
    dict_dtypes_mapped = {}
    for k in dtypes_dict:
        dict_dtypes_mapped[k] = dict_mapping[dtypes_dict[k]]

    # This is required as pandas cannot handle mixed dtypes in Object columns
    dtypes_dict_fixed = {
        k: ("String" if v == "Object" else str(v))
        for k, v in dict_dtypes_mapped.items()
    }

    return dtypes_dict_fixed


@task
def update_dict(d: dict, d_new: dict) -> dict:
    d_copy = copy.deepcopy(d)
    d_copy.update(d_new)
    return d_copy


@task
def df_map_mixed_dtypes_for_parquet(
    df: pd.DataFrame, dtypes_dict: dict
) -> pd.DataFrame:
    """
    Pandas is not able to handle mixed dtypes in the column in to_parquet
    Mapping 'object' visions dtype to 'string' dtype to allow Pandas to_parquet

    Args:
        dict_dtypes_mapped (dict): Data types dictionary inferenced by Visions
        df (pd.DataFrame): input DataFrame.

    Returns:
        df_mapped (pd.DataFrame): Pandas DataFrame with mapped Data Types to workaround Pandas to_parquet bug connected with mixed dtypes in object:.
    """
    df_mapped = df.copy()
    for col, dtype in dtypes_dict.items():
        if dtype == "Object":
            df_mapped[col] = df_mapped[col].astype("string")
    return df_mapped


@task
def update_dtypes_dict(dtypes_dict: dict) -> dict:
    """
    Task to update dtypes_dictionary that will be stored in the schema. It's required due to workaround Pandas to_parquet bug connected with mixed dtypes in object

    Args:
        dtypes_dict (dict): Data types dictionary inferenced by Visions

    Returns:
        dtypes_dict_updated (dict): Data types dictionary updated to follow Pandas requeirments in to_parquet functionality.
    """
    dtypes_dict_updated = {
        k: ("String" if v == "Object" else str(v)) for k, v in dtypes_dict.items()
    }

    return dtypes_dict_updated


@task
def df_to_csv(
    df: pd.DataFrame,
    path: str,
    sep="\t",
    if_exists: Literal["append", "replace", "skip"] = "replace",
    **kwargs,
) -> None:

    """
    Task to create csv file based on pandas DataFrame.
    Args:
    df (pd.DataFrame): Input pandas DataFrame.
    path (str): Path to output csv file.
    sep (str, optional): The separator to use in the CSV. Defaults to "\t".
    if_exists (Literal["append", "replace", "skip"], optional): What to do if the table exists. Defaults to "replace".
    """

    if if_exists == "append" and os.path.isfile(path):
        csv_df = pd.read_csv(path, sep=sep)
        out_df = pd.concat([csv_df, df])
    elif if_exists == "replace":
        out_df = df
    elif if_exists == "skip" and os.path.isfile(path):
        logger.info("Skipped.")
        return
    else:
        out_df = df

    # create directories if they don't exist
    try:
        if not os.path.isfile(path):
            directory = os.path.dirname(path)
            os.makedirs(directory, exist_ok=True)
    except:
        pass

    out_df.to_csv(path, index=False, sep=sep)


@task
def df_to_parquet(
    df: pd.DataFrame,
    path: str,
    if_exists: Literal["append", "replace", "skip"] = "replace",
    **kwargs,
) -> None:
    """
    Task to create parquet file based on pandas DataFrame.
    Args:
    df (pd.DataFrame): Input pandas DataFrame.
    path (str): Path to output parquet file.
    if_exists (Literal["append", "replace", "skip"], optional): What to do if the table exists. Defaults to "replace".
    """
    if if_exists == "append" and os.path.isfile(path):
        parquet_df = pd.read_parquet(path)
        out_df = pd.concat([parquet_df, df])
    elif if_exists == "replace":
        out_df = df
    elif if_exists == "skip":
        logger.info("Skipped.")
        return
    else:
        out_df = df

    # create directories if they don't exist
    try:
        if not os.path.isfile(path):
            directory = os.path.dirname(path)
            os.makedirs(directory, exist_ok=True)
    except:
        pass

    out_df.to_parquet(path, index=False, **kwargs)


@task
def dtypes_to_json(dtypes_dict: dict, local_json_path: str) -> None:
    """
    Creates json file from a dictionary.
    Args:
        dtypes_dict (dict): Dictionary containing data types.
        local_json_path (str): Path to local json file.
    """
    with open(local_json_path, "w") as fp:
        json.dump(dtypes_dict, fp)


@task
def union_dfs_task(dfs: List[pd.DataFrame]):
    """
    Create one DataFrame from a list of pandas DataFrames.
    Args:
        dfs (List[pd.DataFrame]): List of pandas Dataframes to concat. In case of different size of DataFrames NaN values can appear.
    """
    return pd.concat(dfs, ignore_index=True)


@task
def write_to_json(dict_, path):
    """
    Creates json file from a dictionary. Log record informs about the writing file proccess.
    Args:
        dict_ (dict): Dictionary.
        path (str): Path to local json file.
    """
    logger = prefect.context.get("logger")

    if os.path.isfile(path):
        logger.warning(f"File {path} already exists. Overwriting...")
    else:
        logger.debug(f"Writing to {path}...")

    # create parent directories if they don't exist
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, mode="w") as f:
        json.dump(dict_, f)

    logger.debug(f"Successfully wrote to {path}.")


@task
def cleanup_validation_clutter(expectations_path):
    ge_project_path = Path(expectations_path).parent
    shutil.rmtree(ge_project_path)


@task
def generate_table_dtypes(
    table_name: str = None,
    db_name: str = None,
    reserve: float = 1.4,
    config_key: str = None,
    only_dict: bool = True,
    credentials: str = None,
    schema: str = None,
    connection: object = None,
) -> dict:
    """Functon that automaticy generate dtypes dict from SQL table.

    Args:
        table_name (str): Table name. Defaults to None.
        db_name (str): Data base name. Defaults to None.
        reserve (str): How many signs add to varchar, percentage of value. Defaults to 1.4.
        config_key (str): The key inside local config containing the config.
        only_dict (bool): Choose to generate dictionary or whole dataframe. Defaults to True.
        credentials (str, optional): Credentials for the connection. Defaults to None.
        schema (str, optional): Schema name. Defaults to None.
        connection (object, optional): Object of connection to database. Defaults to None.

    Returns:
        Dictionary
    """

    query_admin = f"""select COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, 
            NUMERIC_PRECISION, DATETIME_PRECISION, 
            IS_NULLABLE 
            from INFORMATION_SCHEMA.COLUMNS
            where TABLE_NAME='{table_name}' and TABLE_SCHEMA='{schema}'
            order by CHARACTER_MAXIMUM_LENGTH desc"""
    col_names = [
        "COLUMN_NAME",
        "DATA_TYPE",
        "CHARACTER_MAXIMUM_LENGTH",
        "NUMERIC_PRECISION",
        "DATETIME_PRECISION",
        "IS_NULLABLE",
    ]
    if connection is None:
        sql = AzureSQL(config_key=config_key, credentials=credentials)
        if db_name == True:
            sql.credentials["db_name"] = db_name

        if sql.con == True:
            print("Connection established")
        data = sql.run(query_admin)
        df = pd.DataFrame.from_records(data, columns=col_names)
    elif connection:
        df = pd.read_sql_query(query_admin, connection)

    create_int = lambda x: int(int(x) * reserve / 10) * 10 if int(x) > 30 else 30

    df["CHARACTER_MAXIMUM_LENGTH"] = (
        df["CHARACTER_MAXIMUM_LENGTH"]
        .astype(str)
        .apply(lambda x: str(x.replace(".0", "")))
    )
    df["CHARACTER_MAXIMUM_LENGTH"] = df["CHARACTER_MAXIMUM_LENGTH"].apply(
        lambda x: f"varchar({create_int(x)})" if x.isdigit() else "varchar(500)"
    )
    if only_dict == True:
        return dict(zip(df["COLUMN_NAME"], df["CHARACTER_MAXIMUM_LENGTH"]))
    else:
        return df


@task
def df_converts_bytes_to_int(df):
    logger = prefect.context.get("logger")
    logger.info("Converting bytes in dataframe columns to list of integers")
    return df.applymap(lambda x: list(map(int, x)) if isinstance(x, bytes) else x)


@task
def df_clean_column(df: pd.DataFrame) -> pd.DataFrame:
    if "Customer Requested" in list(df.columns):
        df["Customer Requested"].replace(
            to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"],
            value=["", ""],
            regex=True,
            inplace=True,
        )

    return df


class Git(Git):
    @property
    def git_clone_url(self):
        """
        Build the git url to clone
        """
        if self.use_ssh:
            return f"git@{self.repo_host}:{self.repo}"
        return f"https://{self.git_token_secret}@{self.repo_host}/{self.repo}"
