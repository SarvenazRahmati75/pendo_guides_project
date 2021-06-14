import pandas as pd
import boto3

from authlib.integrations.requests_client import OAuth2Session

from pendoguidesproject.jobs.ingest import ingestPendo
from pendoguidesproject.secrets import Secrets
from pendoguidesproject.config import SecretsConfig
from pendoguidesproject.jobs import entrypoint
from pendoguidesproject.Redshift import redshiftExporter
import io
from io import StringIO
import os
import awswrangler as wr

import smtplib
import datetime
from datetime import timedelta
from datetime import date as dt
from datetime import datetime

@entrypoint("transform")
def run(env: str, date: str):
    os.environ["environment"] = env
    # Get in pendo guide data
    pendoguidesdata = readCSVFromS3("pendoguides")
    pendoguidesdata["date_time"] = pd.to_datetime(pendoguidesdata["browsertime"], unit='ms')

    vid_list=get_visitorlist(pendoguidesdata)
    gid_list=get_guidelist(pendoguidesdata)

    #timeonGuide returns a datafram includes [guide id, visitor id, step id, time on guide,date partition]
    df=timeonGuide(gid_list,vid_list ,pendoguidesdata)

    #write the output dataframe in S3/clean
    writeToCSVToS3(df,"pendoguides")

def get_visitorlist(pendoguidesdata):
     vid_list = list(set(pendoguidesdata["visitorid"]))
     return vid_list

def get_guidelist(pendoguidesdata):
    gid_list = list(set(pendoguidesdata["guideid"]))
    return gid_list

def findHighest(lst, target):
    if len(lst)==0:
        return target
    max_element = min(pd.to_numeric(lst))
    if target < min(pd.to_numeric(lst)):
        return target
    for e in lst:
        if int(e) > max_element and int(e) < target:
            max_element = int(e)

    return max_element

def findLowest(lst, target):
    max_element = max(pd.to_numeric(lst))

    for e in lst:
        if int(e) < max_element and int(e) > target:
            max_element=int(e)
    return max_element


def timeonGuide(gid_list, vid_list,pendoguidesdata):
    output = []
    for gid in gid_list:
        selected_gid = pendoguidesdata[pendoguidesdata["guideid"] == gid]

        for vid in vid_list:

            select_vid = selected_gid[selected_gid["visitorid"] == vid]

            # gid_list = list(set(select["guideid"]))
            gsid_list = list(set(select_vid["guidestepid"]))
            type_list = list(set(select_vid["type"]))
            dismissed = list(set(select_vid["type"]))

            total = 0
            s = 0

            seen = select_vid.loc[select_vid["type"] == "guideSeen"]["browsertime"]
            dismissed = select_vid.loc[select_vid["type"] == "guideDismissed"]["browsertime"]

            for x in dismissed:
                gs = list(select_vid.loc[select_vid["browsertime"] == x]["guidestepid"])[0]
                accountID = list(select_vid.loc[select_vid["browsertime"] == x]["accountid"])[0]
                date = list(select_vid.loc[select_vid["browsertime"] == x]["date_time"])[0]
                output.append([accountID,gid, vid, gs, ((x - findHighest(seen, x)) / 1000), date])
                total += (x - findHighest(seen, x)) / 1000

            # total=sum(output)/1000
            advance_list = list(select_vid.loc[select_vid["type"] == "guideAdvanced"]["browsertime"])

            for gsid in gsid_list:
                m = 0
                select_gsid = select_vid.loc[(select_vid["guidestepid"] == gsid)]
                seen = pd.to_numeric(list(select_gsid.loc[select_gsid["type"] == "guideSeen"]["browsertime"]))
                dismissed = pd.to_numeric(list(select_gsid.loc[select_gsid["type"] == "guideDismissed"]["browsertime"]))
                advance = pd.to_numeric(list(select_gsid.loc[select_gsid["type"] == "guideAdvanced"]["browsertime"]))
                activity = pd.to_numeric(list(select_gsid.loc[select_gsid["type"] == "guideActivity"]["browsertime"]))

                if len(advance) > 0:
                    for a in advance:
                        highest_seen = findHighest(seen, a)
                        lowest_advanced = findLowest(advance, highest_seen)
                        date = list(select_gsid.loc[select_gsid["browsertime"] == a]["date_partition"])[0]

                        if lowest_advanced == a:
                            m += (int(a) - int(findHighest(seen, a))) / 1000
                            output.append([accountID,gid, vid, gsid, m, date])

                if len(activity) > 0 and len(dismissed) == 0 and len(advance) == 0:
                    for a in activity:
                        m += (int(a) - int(findHighest(seen, a))) / 1000

                total += m
                # output.append([gsid,m])

    df = pd.DataFrame.from_records(output, columns=["account id","guideid", "visitorid", "guidestepid", "time on guide", "date_partition"])
    return df


def writeToCSVToS3(dataset, description):
    env = os.environ["environment"]
    csv_buffer = StringIO()
    dataset.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(get_bucket(env), f"clean/pendoguides/{description}.csv").put(Body=csv_buffer.getvalue())

def get_bucket(env: str):
    return 'cdo-datalake-prd' if env == 'prd' else 'cdo-datalake-dev-bphcob'

def readCSVFromS3(description):
    env = os.environ["environment"]
    bucket = get_bucket(env)
    key = f"raw/pendoguides/{description}.csv"
    s3_resource = boto3.client('s3')
    obj = s3_resource.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
    return df
