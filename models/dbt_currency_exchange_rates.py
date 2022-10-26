from datetime import datetime, timedelta
import json
from typing import Dict


def get_schema(profile_type: str):
    if profile_type == "databricks":
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
    # lookback_days = dbt.config.get("lookback_days")
    # test_config = str(dbt.config.get("test_config"))
    # lookback_days, profile_type, test_config = dbt.config.get("lookback_days", "profile_type", "test_config")

    # profile_type = str(dbt.config.get("profile_type"))

    profile_type, lookback_days, starsnow_functions_schema = [
        x.strip() for x in dbt.config.get("config_data").split("|")
    ]

    dbt.config(
        materialized="incremental",
        packages=["requests"],
        unique_key=["currency_code", "date"],
    )

    if dbt.is_incremental:
        start = (datetime.utcnow() - timedelta(int(lookback_days))).strftime("%Y-%m-%d")
    else:
        start = datetime(2000, 1, 1).strftime("%Y-%m-%d")

    end = datetime.utcnow().strftime("%Y-%m-%d")

    if profile_type == "databricks":
        import requests

        r = requests.get(
            f"https://api.exchangerate.host/timeseries?start_date={start}&end_date={end}"
        )
        df = spark.createDataFrame(prep_dict_for_dataframe(r.json()), get_schema(profile_type))

    elif profile_type == "snowflake":
        from snowflake.snowpark.functions import call_udf, col

        # Snowpark does not support external http calls: https://community.snowflake.com/s/question/0D53r0000BeAAgHCQW/error-in-calling-rest-api-endpoint-using-requests-get-post-method-in-snowpark-python-stored-procedure
        # As a proof-of-concept, the STARSNOW_REQUEST function can be installed and used to perform http calls: https://github.com/starschema/starsnow_request#deploying

        starsnow_params = session.create_dataframe(
            [
                [
                    f"https://api.exchangerate.host/timeseries?start_date={start}&end_date={end}",
                    {
                        "method": "get",
                    },
                ]
            ],
            schema=["url", "params"],
        )

        df = starsnow_params.select(
            call_udf(
                f"{starsnow_functions_schema}.STARSNOW_REQUEST",
                col("url"),
                col("params"),
            ).as_("response")
        )
        rates_dict = json.loads(
            df.select("response").collect()[0].as_dict()["RESPONSE"]
        )["data"]

        df = session.create_dataframe(
            prep_dict_for_dataframe(rates_dict), get_schema(profile_type)
        )

    else:
        raise ValueError("Only Databricks and Snowflake are supported.")

    return df
