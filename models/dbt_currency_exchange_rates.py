from datetime import datetime, timedelta


def model(dbt, session):
    # lookback_days = dbt.config.get("lookback_days")
    # test_config = str(dbt.config.get("test_config"))
    # lookback_days, profile_type, test_config = dbt.config.get("lookback_days", "profile_type", "test_config")

    profile_type = str(dbt.config.get("profile_type"))

    profile_type, lookback_days = [
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
        from pyspark.sql.types import StructField, StructType, StringType, FloatType

        r = requests.get(
            f"https://api.exchangerate.host/timeseries?start_date={start}&end_date={end}"
        )

        rates = []
        for d in list(r.json()["rates"].keys()):
            for k, v in r.json()["rates"][d].items():
                rates.append({"date": d, "currency_code": k, "rate": float(v)})

        schema = StructType(
            [
                StructField("date", StringType(), False),
                StructField("currency_code", StringType(), False),
                StructField("rate", FloatType(), False),
            ]
        )

        df = spark.createDataFrame(rates, schema)

    elif profile_type == "snowflake":
        from snowflake.snowpark.types import (
            StructField,
            StructType,
            StringType,
            FloatType,
        )

        # Snowpark does not support externall http calls: https://community.snowflake.com/s/question/0D53r0000BeAAgHCQW/error-in-calling-rest-api-endpoint-using-requests-get-post-method-in-snowpark-python-stored-procedure
        # As a proof-of-concept, just going to send some hard-codded values to Snowflake

        # r = requests.get(f"https://api.exchangerate.host/timeseries?start_date={start}&end_date={end}")

        rates = [
            ["2022-09-16", "AED", 3.677051],
            ["2022-09-16", "AFN", 89.086493],
            ["2022-09-16", "ALL", 116.612837],
        ]

        df = session.create_dataframe(rates, schema=["date", "currency_code", "rate"])
    else:
        raise ValueError("Only Databricks and Snowflake are supported.")

    return df
