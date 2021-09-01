import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd
import prefect
from prefect import task
from visions.functional import infer_type
from visions.typesets.complete_set import CompleteSet

METADATA_COLUMNS = {"_viadot_downloaded_at_utc": "DATETIME"}


@task
def add_ingestion_metadata(
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
def chunk_df(df: pd.DataFrame, size: int = 10_000) -> List[pd.DataFrame]:
    n_rows = df.shape[0]
    chunks = [df[i : i + size] for i in range(0, n_rows, size)]
    return chunks


@task
def get_df_dtypes(df: pd.DataFrame) -> Dict[str, str]:
    typeset = CompleteSet()
    dtypes = infer_type(df, typeset)
    dtypes_dict = {k: str(v) for k, v in dtypes.items()}
    return dtypes_dict


@task
def dict_to_json(dict_: dict, path: str) -> None:

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
def df_to_parquet(df, path: str, if_exists: str = "replace"):
    if if_exists == "append":
        if os.path.isfile(path):
            parquet_df = pd.read_parquet(path)
            out_df = pd.concat([parquet_df, df])
        else:
            out_df = df
    elif if_exists == "replace":
        out_df = df
    out_df.to_parquet(path, index=False)
