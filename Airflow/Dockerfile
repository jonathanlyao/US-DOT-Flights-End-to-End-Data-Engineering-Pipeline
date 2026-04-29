FROM apache/airflow:2.10.4-python3.11

USER airflow

RUN pip install --no-cache-dir \
    dbt-snowflake==1.11.4 \
    boto3 \
    requests \
    snowflake-connector-python
