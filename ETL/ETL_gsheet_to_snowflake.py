import os.path
import pandas as pd
import re

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import date

def updateReportDetails(report_id,end_date,cur,destination,reportname,SPREADSHEET_ID='1MHqiaZ3ikxvEQAvJ2lw2m7bo__MOBzXgpR_7nsclA6U',RANGE_NAME='InfoLookup!A:U'): 

    # If modifying these scopes, delete the file token.json.
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    SAMPLE_SPREADSHEET_ID=SPREADSHEET_ID
    SAMPLE_RANGE_NAME=RANGE_NAME
    today=date.today()
    today=today.strftime("%Y-%m-%d")

    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                '/Users/cyndiz/Documents/Projects/credentials/gsheets.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    try:
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                    range=SAMPLE_RANGE_NAME).execute()
        values = result.get('values', [])
        # values[0]
        header=values[0]
        value=values[1:]
        df1 = pd.DataFrame(value,columns=header)
        # print(df1)
        if (end_date=='null'):
            df=df1.loc[(df1['Report ID'].isin(report_id))]
        else:    
            df=df1.loc[(df1['Report ID'].isin(report_id))&(df1['End Date']==end_date)]
        # Compile from the Gsheet
        df['Scope']=df['Scope'].apply(lambda x: x.split(", "))
        df['DMA Filtering']=df['DMA Filtering'].apply(lambda x: x.split("\n"))
        df['Audience IDs']=df['Audience IDs'].apply(lambda x: x.split("\n"))
        df['Kantar Advertiser ID']=df['Kantar Advertiser ID'].apply(lambda x: x.split("\n"))
        df['Kantar Product ID']=df['Kantar Product ID'].apply(lambda x: x.split("\n"))
        df['Vizio Fingerprint IDs']=df['Vizio Fingerprint IDs'].apply(lambda x: re.sub(r'"','',x).split("\n"))
        df['Youtube Ad IDs']=df['Youtube Ad IDs'].apply(lambda x: re.sub(r'"','',x).split("\n"))
        df['Facebook Campaign ID']=df['Facebook Campaign ID'].apply(lambda x: re.sub(r'"','',x).split("\n"))
        df['Conversion Pixel IDs']=df['Conversion Pixel'].apply(lambda x: re.findall(r'\d+',x))
        df['Conversion Pixel Names']=df['Conversion Pixel'].apply(lambda x: re.sub(r'[^\D+]','',x).split("\n"))
        df['Conversion Pixel']=df['Conversion Pixel'].apply(lambda x: x.split("\n"))
        df['DMA Codes']=df['DMA Codes'].apply(lambda x: x.split("\n"))
        df['updated_date']=today
        # print(df['Vizio Fingerprint IDs'])
        # print(df['Kantar Product ID'])
        if not values:
            print('No data found.')

    except HttpError as err:
        print(err)

    # print(df)
    # cols = list(df1.columns.values)
    # print(cols)
    df=df[['ReportName','Report ID', 'Client', 'Client_param', 'Scope', 'DMA Filtering','Start Date', 'End Date', 'Audience IDs', 
         'Attribution Window', 'Kantar Advertiser ID', 'Kantar Product ID', 'Vizio Fingerprint IDs', 'Youtube Ad IDs', 
         'Facebook Pixel ID', 'Facebook Account ID','Facebook Campaign ID', 'Conversion Pixel', 'DMA Codes','Conversion Table', 'Campaign Weight Table', 'Mapped Final Union Table',
         'Conversion Pixel IDs', 'Conversion Pixel Names', 'updated_date']]

    filepath=os.path.dirname(os.path.realpath('__file__'))
    filename=os.path.join(filepath,'{reportname}_{date}.csv').format(reportname=reportname, date=today)
    df.to_csv(filename, index=False, sep=';')
    report_ids=','.join(report_id)

    # print(report_ids)
    cur.execute('''create or replace file format myformat type='CSV' field_delimiter=';' skip_header=1;''')
    cur.execute('''create or replace stage my_int_stage_1 file_format=myformat copy_options = (on_error='skip_file');''')
    qry_put= '''PUT file://{filename} @my_int_stage_1;'''.format(filename=filename)
    qry_create = '''
    --CREATE OR REPLACE TABLE {tablename} (
    CREATE TABLE IF NOT EXISTS {tablename} (
        report_name                 varchar,
        report_id                   int,
        client_name                 varchar(50),
        client_param                varchar(10),
        report_scope                array,
        dma_filter                  array,
        start_date                  int,
        end_date                    int,
        audience_id                 array,
        attribution_window          int,
        kantar_adveritser_id        array,
        kantar_product_id           array,
        vizio_fingerprint_id        array,
        youtube_ad_id               array,
        fb_pixel_id                 bigint,
        fb_account_id               bigint,
        fb_campaign_id              array,
        conversion_pixel            array,
        dma_code                    array,
        conversion_table            varchar,
        campaign_weight_table       varchar,
        mapped_final_union_table    varchar,
        conversion_pixel_id         array,
        conversion_pixel_name       array,
        updated_date                date
    ) CLUSTER BY (report_id);'''.format(tablename=destination)
    if (end_date=='null'):
        qry_delete = '''DELETE FROM {tablename} WHERE report_id in ({report_id});'''.format(tablename=destination,report_id=report_ids)
    else:
        qry_delete = '''DELETE FROM {tablename} WHERE report_id in ({report_id}) and end_date={end_date};'''.format(tablename=destination, report_id=report_ids, end_date=end_date)
    
    qry_copy = '''COPY INTO {tablename} from @my_int_stage_1 '''.format(tablename=destination)
    
    cur.execute(qry_put)
    cur.execute(qry_create)
    cur.execute(qry_delete)
    cur.execute(qry_copy)
    cur.execute('select * from {tablename} where report_id in ({report_id});'.format(tablename=destination,report_id=report_ids))

    rows=cur.fetch_pandas_all()
    display(rows)
