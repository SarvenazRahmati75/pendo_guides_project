from time import sleep

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
import awswrangler as wr

def export_activity():
    # Get in cleaned data
    activity = readCSVFromS3("pendoguides")
    # Write back data
    writeToRedshift(activity, "guide_usage")

def get_bucket(env: str):
    return 'cdo-datalake-prd' if env == 'prd' else 'cdo-datalake-dev-bphcob'

def readCSVFromS3(description):
    env = os.environ["environment"]
    bucket = get_bucket(env)
    key = f"clean/pendoguides/{description}.csv"
    s3_resource = boto3.client('s3')
    obj = s3_resource.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
    return df

def writeToRedshift(df, description):
    env = os.environ["environment"]
    # Write to Glue
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    wr.s3.to_parquet(
        df=df,
        path="s3://" + get_bucket(os.environ["environment"]) + "/master/pendoguides/",
        dataset=True,
        database= "pendoguides" if env == "prd" else "pendoguides_dev",
        table=description,
        mode="overwrite",
    )
    # Write to Redshift
    exporter = redshiftExporter("pendoguides", description, False)
    exporter.export()

# import argparse
# import logging
# import os
# import sys
# from time import sleep
# import boto3
# from pendoguidesproject.Redshift import redshiftExporter
#
# def export(database, dataset, historical_load) -> None:
#     exporter = redshiftExporter(database, dataset, historical_load)
#     exporter.export()
# if __name__ == "__main__":
#     sleep(5)
#     export("pendoguides", "pendoguides", False)

if __name__ == "__main__":
    sleep(5)
    print("inside main")
    # Export
    export_activity()
