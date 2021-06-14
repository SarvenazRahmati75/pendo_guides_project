import logging
import requests
from typing import Optional
import time
import pandas as pd
import boto3
from authlib.integrations.requests_client import OAuth2Session
from pendoguidesproject.secrets import Secrets
from pendoguidesproject.config import SecretsConfig
from pendoguidesproject.jobs import entrypoint
from pendoguidesproject.Redshift import redshiftExporter
import io
from io import StringIO
import os
import psycopg2
from datetime import datetime
import pytz
import awswrangler as wr


@entrypoint("ingest")
def run(env: str, date: str):
    os.environ["environment"] = env
    time.sleep(5)

    ingestData()

def ingestData():
    pendo_guides = ingestPendo()
    writeToCSVToS3(pendo_guides,"pendoguides")
    # Enhance readability
    return pendo_guides

def ingestPendo():
    # Engine
    importer = redshiftExporter("pendoguides", "", False)
    con = importer.import_data()
    con.set_isolation_level("ISOLATION_LEVEL_AUTOCOMMIT")
    # Ingest pendo guides
    sql = "select * from pendo.guides_usage"
    pendo_guides = pd.read_sql_query(sql, con)
    # Close connection
    con.close()
    return pendo_guides

def writeToCSVToS3(dataset, description):
    env = os.environ["environment"]
    csv_buffer = StringIO()
    dataset.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(get_bucket(env), f"raw/pendoguides/{description}.csv").put(Body=csv_buffer.getvalue())

def readCSVFromS3(description):
    env = os.environ["environment"]
    bucket = get_bucket(env)
    key = f"raw/pendoguides/{description}.csv"
    s3_resource = boto3.client('s3')
    obj = s3_resource.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
    return df

def get_bucket(env: str):
    return 'cdo-datalake-prd' if env == 'prd' else 'cdo-datalake-dev-bphcob'
