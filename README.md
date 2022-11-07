A ~~one-day~~ multiple-day exploration of python models in dbt ~~(still in beta)~~ and how a dbt package can cater to multiple databases.

## Objectives

- Create a python model that retrieves currency exchange rates from an open source API (i.e. no requirement for credentials).
- Save this data to a table, allowing for incremental refreshes.
- Create a dbt package that supports mulitple databases (in order of preference: Databricks, Snowflake, BigQuery).

## Results

- Databricks is relatively simple to support.
- Snowflake does not allow external http requests (or non-Anaconda packages) yet, see [here](https://community.snowflake.com/s/question/0D53r0000BeAAgHCQW/error-in-calling-rest-api-endpoint-using-requests-get-post-method-in-snowpark-python-stored-procedure), although a workaround is available, detailed below.
- BigQuery was not attempted.
- Python models in dbt still have a number of rough edges (lack of logging/printing options~~, bug in passing config values to model~~, etc.).


## Additional setup required for BigQuery

1. Create or use an existing Dataproc cluster with the S[park BigQuery connector](https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/connectors#bigquery-connectors) initialization action.

## Additional setup required for Snowflake

1. Enable Anaconda python packages as described [here](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#using-third-party-packages-from-anaconda).

1. Install the STARSNOW_REQUEST functions as detailed [here](https://github.com/starschema/starsnow_request#deploying).

## How to use this package in your dbt project

1. Create a `packages.yml` file in your dbt directory.
1. Add the following:
    ```yml
    packages:
    - git: https://github.com/pgoslatara/dbt-currency-exchange-rates.git
    ```
1. Run `dbt deps` to install.
1. In your `dbt_project.yml` file add the following variables (all are optional and can be omitted):
    ```yml
    vars:
        dbt_currency_exchange_rates_lookback_day: 3
        dbt_currency_exchange_rates_schema: src_exchange_rates
        starsnow_functions_schema: PUBLIC
    ```
    - `dbt_currency_exchange_rates_lookback_day`: The number of days data that will be fetched for incremental runs.
    - `dbt_currency_exchange_rates_schema`: The schema where the `dbt_currency_exchange_rates` model will be saved.
    - `starsnow_functions_schema`: The Snowflake schema where the STARSNOW_REQUEST functions are available.
1. Run `dbt build dbt_currency_exchange_rates` to create a table in your database with currency exchange rates.

## Development

1. Create a virtual environment, preferably using python 3.8+.
1. Run:
    ```bash
    pip install -r requirements.txt
    ```
1. Copy `.envrc.example` to `.envrc` and populate with credentials/configs for Databricks and Snowflake.
1. Source this file:
    ```bash
    source .envrc
    ```
1. Make any desired changes and run with the appropriate target:
    ```bash
    dbt build --target databricks
    ```
    Or:
    ```bash
    dbt build --target snowflake
    ```
