import math
import os
import random
import string
import time
from datetime import datetime, timedelta
from pathlib import Path
from string import Template
import boto3

import pandas as pd
import sqlalchemy as db

from kumparde.common import config, get_db_connection
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

filepath = os.path.dirname(os.path.abspath(__file__ + '/../')) + '/file/'


def file_to_s3(filepath: str, s3_key: str, s3_bucket: str) -> str:
    filename = os.path.basename(filepath)
    key = f'{s3_key}/{filename}'
    s3 = boto3.client(
        's3'
    )
    s3.upload_file(
        Bucket=s3_bucket,
        Filename=filepath,
        Key=key
    )
    return key


def df_to_redshift(data_df: pd.DataFrame, db_connection: db.engine.Connection, table_name: str,
                   schema_name: str) -> None:
    path = f'/tmp/{schema_name}_{table_name}_{round(time.time())}.gzip'
    data_df.to_csv(path, header=False, index=False, compression='gzip', doublequote=True, float_format='%.12g')

    str_column = ", ".join([f'"{each}"' for each in data_df.columns])
    str_column = "(" + str_column + ")"

    s3_bucket = config("/s3/bucket")
    s3_dir = f'etl/{schema_name}/{table_name}'
    s3_secret_key = config("/s3/secret_key")
    s3_access_key = config("/s3/access_key")
    key = file_to_s3(path, s3_dir, s3_bucket)
    s3_to_redshift(key, s3_access_key, s3_secret_key, s3_bucket, table_name, schema_name, db_connection,
                   str_column)
    os.remove(path)


def get_data_detail(db_connection: db.engine.Connection, db_type: str, date_column: str,
                    schema_name: str, table_name: str, min_date: str = None) -> dict:
    sql_filename = f"{db_type}_get_data_detail.sql"
    date_condition = ""
    if min_date:
        date_condition = f" AND {date_column} >= '{min_date}'::date"

    query = Template(Path(f'{filepath}/{sql_filename}').read_text()).substitute(
        schema_name=schema_name,
        table_name=table_name,
        date_column=date_column,
        date_condition=date_condition
    )

    detail = pd.read_sql(db.text(query), db_connection)
    detail = detail.iloc[0].to_dict()
    return detail


def s3_to_redshift(s3_key: str, access_key: str, secret_key: str, s3_bucket: str, table_name: str, table_schema: str,
                   db_connection: db.engine.Connection, list_column: str = None) -> None:
    logger.info(f's3_to_redshift, event=start, table_name={table_schema}.{table_name}, '
                f's3_key=s3://{s3_bucket}/{s3_key}')

    query = Template(Path(f'{filepath}/copy_s3_to_redshift.sql').read_text())

    if not list_column:
        list_column = ''

    query = query.substitute(
        table=table_name,
        schema=table_schema,
        bucket=s3_bucket,
        key=s3_key,
        access_key=access_key,
        secret_key=secret_key,
        list_column=list_column
    )

    db_connection.execute(db.text(query))
    logger.info(f's3_to_redshift, event=end, status=success, table_name={table_schema}.{table_name},'
                f' 'f's3_key=s3://{s3_bucket}/{s3_key}')


def replicate(source_db_type: str, source_schema: str, source_table: str,
              target_db_type: str, target_schema: str, target_table: str,
              date_column: str = 'updated_at') -> None:
    logger.info(f'replicate, event=start, source_table={source_schema}.{source_table}, '
                f'target_table={target_schema}.{target_table}')

    source_db_connection = get_db_connection(db_type=source_db_type, db_name=source_schema)
    target_db_connection = get_db_connection(db_type=target_db_type, db_name="dev")

    # get max date from target table
    # ideally, when target table does not exist, there is a process to create the table
    # but for now, let's just manually create the target table
    target_data_detail = get_data_detail(db_connection=target_db_connection,
                                         db_type="redshift",
                                         schema_name=target_schema,
                                         table_name=target_table,
                                         date_column=date_column)
    logger.info(f'replicate, message="get data detail from target table", target_data_detail={target_data_detail}, '
                f'table_name={target_schema}.{target_table}')

    source_data_detail = get_data_detail(schema_name=target_schema,
                                         db_type="mysql",
                                         table_name=target_table,
                                         db_connection=source_db_connection,
                                         date_column=date_column)
    logger.info(f'replicate, message="get data detail from source table", source_data_detail={source_data_detail}, '
                f'table_name={target_schema}.{target_table}')

    if source_data_detail['row_count'] <= 0:
        logger.info(f'replicate, event=end, message="no data from source"')
        return

    date_condition = ''
    if target_data_detail['row_count'] > 0:
        temp_min_date = target_data_detail["max_date"]
        date_condition = f" AND {date_column} >= date '{temp_min_date}'"

    query = Template(Path(f'{filepath}/replicate_articles.sql').read_text()).substitute(
        date_column=date_column,
        date_condition=date_condition
    )
    source_data_df = pd.read_sql(db.text(query), source_db_connection)

    staging_schema = 'staging'
    staging_table = f'{target_schema}_{target_table}'
    target_db_connection.execute(db.text(f'truncate table {staging_schema}.{staging_table}'))
    df_to_redshift(data_df=source_data_df, db_connection=target_db_connection, schema_name=staging_schema,
                   table_name=staging_table)

    query = Template(Path(f'{filepath}/merge_query.sql').read_text()).substitute(
        source_schema=staging_schema,
        source_table=staging_table,
        target_schema=target_schema,
        target_table=target_table,
        join_column=f'{target_table}.id = source_table.id',
        source_columns=','.join([f'source_table.{each}' for each in source_data_df.columns]),
        target_columns=','.join(source_data_df.columns),
        matched_columns=','.join([f'{each}=source_table.{each}' for each in source_data_df.columns])
    )
    logger.info(f'replicate, message="merge query", query={query}')
    target_db_connection.execute(db.text(query))


# if __name__ == "__main__":
#     replicate(source_db_type="mysql",
#               source_schema="kumparde",
#               source_table="articles",
#               target_db_type="redshift",
#               target_schema="kumparde",
#               target_table="articles")
