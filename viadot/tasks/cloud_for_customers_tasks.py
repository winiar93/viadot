from prefect import task
import pandas as pd
from ..sources import CloudForCustomers
from typing import Any, Dict, List


@task
def c4c_report_to_df(report_url: str, source_type: str = "QA", skip=0, top=1000):
    final_df = pd.DataFrame()
    next_batch = True
    iteration = 0
    while next_batch:
        new_url = f"{report_url}&$top={top}&$skip={skip}"
        chunk_from_url = CloudForCustomers(report_url=new_url, source_type=source_type)
        df = chunk_from_url.to_df()
        final_df = final_df.append(df)
        if not final_df.empty:
            df_count = df.count()[1]
            if df_count != top:
                next_batch = False
            skip += top
            iteration += 1
        else:
            break
    return final_df


@task
def c4c_to_df(
    url: str = None,
    endpoint: str = None,
    report_url: str = None,
    fields: List[str] = None,
    params: Dict[str, Any] = {},
    source_type: str = "QA",
    if_empty: str = "warn",
):
    cloud_for_customers = CloudForCustomers(
        url=url,
        report_url=report_url,
        endpoint=endpoint,
        params=params,
        source_type=source_type,
    )

    df = cloud_for_customers.to_df(if_empty=if_empty, fields=fields)

    return df
