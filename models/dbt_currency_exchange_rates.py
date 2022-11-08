from datetime import datetime, timedelta, timezone
import json
from typing import Dict


def get_schema(profile_type: str):
    if profile_type in ("bigquery", "databricks"):
        from pyspark.sql.types import StructField, StructType, StringType, FloatType
    elif profile_type == "snowflake":
        from snowflake.snowpark.types import (
            StructField,
            StructType,
            StringType,
            FloatType,
        )

    return StructType(
        [
            StructField("date", StringType(), False),
            StructField("currency_code", StringType(), False),
            StructField("rate", FloatType(), False),
        ]
    )


def prep_dict_for_dataframe(data: Dict) -> Dict:
    rates = []
    for d in list(data["rates"].keys()):
        for k, v in data["rates"][d].items():
            rates.append({"date": d, "currency_code": k, "rate": float(v)})

    return rates


def model(dbt, session):
    profile_type = str(dbt.config.get("profile_type"))
    lookback_days = str(dbt.config.get("lookback_days"))
    starsnow_schema = str(dbt.config.get("starsnow_schema"))

    dbt.config(
        materialized="incremental",
        packages=["requests"],
        unique_key=["currency_code", "date"],
    )

    if dbt.is_incremental:
        start = datetime.now(timezone.utc) - timedelta(int(lookback_days))
    else:
        start = datetime(2000, 1, 1, tzinfo=timezone.utc)

    end = datetime.now(timezone.utc)

    # API returns a maximum of 365 days of data per call, chunking to allow multiple calls
    date_list = [start + timedelta(days=x) for x in range((end - start).days)]
    date_chunks = date_list[::365] + [end]

    if profile_type in ("bigquery", "databricks"):
        import requests

        for date in date_chunks[:-1]:
            r = requests.get(
                f"https://api.exchangerate.host/timeseries?start_date={date.strftime('%Y-%m-%d')}&end_date={date_chunks[date_chunks.index(date) + 1].strftime('%Y-%m-%d')}"
            )
            df_requests = spark.createDataFrame(
                prep_dict_for_dataframe(r.json()), get_schema(profile_type)
            )

            if "df" not in locals():
                df = df_requests
            else:
                df = df.union(df_requests).distinct()

    elif profile_type == "snowflake":
        from snowflake.snowpark.functions import call_udf, col

        # Snowpark does not support external http calls: https://community.snowflake.com/s/question/0D53r0000BeAAgHCQW/error-in-calling-rest-api-endpoint-using-requests-get-post-method-in-snowpark-python-stored-procedure
        # As a proof-of-concept, the STARSNOW_REQUEST function can be installed and used to perform http calls: https://github.com/starschema/starsnow_request#deploying

        for date in date_chunks[:-1]:
            starsnow_params = session.create_dataframe(
                [
                    [
                        f"https://api.exchangerate.host/timeseries?start_date={date.strftime('%Y-%m-%d')}&end_date={date_chunks[date_chunks.index(date) + 1].strftime('%Y-%m-%d')}",
                        {
                            "method": "get",
                        },
                    ]
                ],
                schema=["url", "params"],
            )
            df_requests = starsnow_params.select(
                call_udf(
                    f"{starsnow_schema}.STARSNOW_REQUEST",
                    col("url"),
                    col("params"),
                ).as_("response")
            )
            rates_dict = json.loads(
                df_requests.select("response").collect()[0].as_dict()["RESPONSE"]
            )["data"]

            df_prepped = session.create_dataframe(
                prep_dict_for_dataframe(rates_dict), get_schema(profile_type)
            )

            if "df" not in locals():
                df = df_prepped
            else:
                df = df.union(df_prepped).distinct()

    else:
        raise ValueError("Only BigQuery, Databricks and Snowflake are supported.")

    return df
