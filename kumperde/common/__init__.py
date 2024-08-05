import boto3
import sqlalchemy as db
from urllib.parse import urlparse

def config(parameter_name: str):
    ssm = boto3.client('ssm',
                       'ap-southeast-1')
    parameter = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
    return parameter['Parameter']['Value']


def get_db_connection(db_type: str, db_name: str) -> db.engine.Connection:
    conn_string = config(f'/db/{db_type}/{db_name}')

    engine = db.create_engine(conn_string, isolation_level="AUTOCOMMIT", pool_pre_ping=True)
    connection = engine.connect()
    return connection
