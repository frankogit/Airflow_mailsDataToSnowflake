import requests
import io
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG, Variable
from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator,ShortCircuitOperator
from airflow.macros.plugins import send_alert
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import boto3
from datetime import datetime, timedelta,date
import pandas as pd
import shutil
import os
import csv
import ast
import imaplib
import email
import email.utils
import time
from pathlib import Path

#variables
dag_owner = 'franko'
dag_name = 'DEMO_REC_ingest'
test = False

vars = Variable.get('DEMO_REC_INGEST_CONFIG', deserialize_json = True)
input_files_path = vars['input_files_path']  # Excel files path
local_dir = vars['local_dir']  # csv files path
aws_access_key_id = Variable.get('AWS_ACCESS_KEY_ID') 
aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')
snowflake_hook = SnowflakeHook(snowflake_conn_id = "snowflake_conn", database = 'DEMO')

if not os.path.exists(input_files_path):
    os.makedirs(input_files_path)

if not os.path.exists(local_dir):
    os.makedirs(local_dir)

@send_alert(test = test)
def look_for_new_feeds(*args, **kwargs):
    """
    Function
    ----------------------
    Monitor Office 365 Outlook Inbox,

    """
    ti = kwargs['ti']
        
    # Connect with the Email Inbox
    mail = imaplib.IMAP4_SSL('outlook.office365.com', port=993)
    mail.login(Variable.get("EMAIL_FROMADDR"), Variable.get("EMAIL_FROMADDR_PASSWORD"))
    mail.select("INBOX")
    
    # Search for the emails from the organization
    typ, data = mail.search(None, "(FROM DEMOXX@realestateconsulting.com)")
    id_list = str(data[0], encoding="utf8").split()
    
    found_new_feed = False
    subject = None
    
    # Iterate all the emails
    for mail_id in id_list:
        typ, data = mail.fetch(mail_id, "(RFC822)")
        text = data[0][1]
        msg = email.message_from_bytes(text)
       
        subject = msg["Subject"]

        # if the email is not for feed update, skip it
        currently_date = f'{date.today().strftime("%B")[0:3]}-{date.today().year}'
        feed_update = f'Updated files from DEMO DEMO REC (STARTAPP, {currently_date})' in subject
        # feed_update = "Updated files from DEMO DEMO REC (STARTAPP, Oct-2020)" in subject
        if not feed_update:
            continue
            
        #If the email is for feed, extract the date of the email and verify whether it was already retrieved or not.
        # prepare the information for the tracker file
        retrieve_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        email_datetime = datetime.strptime(time.strftime("%Y-%m-%d %H:%M:%S", email.utils.parsedate(msg["Date"])), "%Y-%m-%d %H:%M:%S").strftime('%Y%m')
        
        # if the email has already been retrieved, skip it        

        print(f"select count(*) from DEMO.information_schema.tables where table_schema = 'RAWDATA' AND TABLE_NAME LIKE '%{email_datetime}' AND  TABLE_NAME LIKE any ('METRO_LEVEL_STATS_%', 'NATIONAL_LEVEL_STATS_%', 'METRO_BHVI_MONTHLY_PROJECTIONS_%','METRO_BHVI_YEARLY_CHANGE_%')")
        check_date = snowflake_hook.get_records(f"select count(*) from DEMO.information_schema.tables where table_schema = 'RAWDATA' AND TABLE_NAME LIKE '%{email_datetime}' AND  TABLE_NAME LIKE any ('METRO_LEVEL_STATS_%', 'NATIONAL_LEVEL_STATS_%', 'METRO_BHVI_MONTHLY_PROJECTIONS_%','METRO_BHVI_YEARLY_CHANGE_%')")
        print(check_date)
        if ( int(check_date[0][0])  < 4):
            found_new_feed = True
            break
        else:
            continue
            
    mail.close()
    mail.logout()
            
    if (found_new_feed == True):
        ti.xcom_push(key = "email_date", value = email_datetime)
        ti.xcom_push(key = 'subject', value = subject)
        ti.xcom_push(key= 'retrieval_date', value = retrieve_datetime)
        ti.xcom_push(key = 'msg', value = msg)
        print(f"Found new feeds with date: {email_datetime}")
        return True
    else:
        print("No feed updates found")
        return False

@send_alert(test = test)
def download_new_feed(logger, *args,**kwargs):
    
    ti = kwargs['ti']
    msg = ti.xcom_pull(task_ids = f"look_for_new_feeds", key = 'msg')
    
    # clean xlsx tmp directory
    files_xlsx = os.listdir(input_files_path)
    for files_rm in files_xlsx:
        os.remove(f"{input_files_path}/{files_rm}")

    # clean csv tmp directory
    files_csv = os.listdir(local_dir)
    for files_rm in files_csv:
        os.remove(f"{local_dir}/{files_rm}")

    # downloading attachments
    for part in msg.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue
        file_name = part.get_filename()
    
        if bool(file_name):
            if not os.path.isdir(input_files_path):
                os.makedirs(input_files_path)
            with open(f"{input_files_path}/{file_name}", "wb") as f:
                f.write(part.get_payload(decode=True))

        logger.info("File downloaded successfully!")

@send_alert(test = test)
def process_df(logger,input_file_name,**kwargs):
    ti = kwargs['ti']
    updated_columns = {}
    output_file_name = input_file_name.replace(" ","_").lstrip("(").rstrip(")").replace("xlsx","csv").replace("-","_")
    if input_file_name.split('2')[0]=="DEMO_InfoRequest_":
        
        df = pd.read_excel("{}/{}".format(input_files_path,input_file_name),sheet_name="SF Rental Slides Info",skiprows=1)
    elif input_file_name.split('2')[0]=="DEMO_InfoRequest_BHVI-":
        
        df = pd.read_excel("{}/{}".format(input_files_path,input_file_name),sheet_name="DEMO Home Value Index (BHVI)",usecols='A:H')
    elif input_file_name.split('2')[0]=="DEMO_InfoRequest_NAT-":
        
        df = pd.read_excel("{}/{}".format(input_files_path,input_file_name),sheet_name="US Data",usecols='A:R')
    df = df.dropna(how='all', axis='columns')    
            
    for column in list(df.columns):
        new_column = {column:column.replace(" ","_").lstrip("(").rstrip(")").replace("xlsx","csv").replace("-","_").replace("(","").replace(")","").replace(".","").upper()}
        updated_columns.update(new_column)

    new_columns = updated_columns   

    df.rename(columns = new_columns,inplace=True)
    logger.info("The number of records are in {} are:{}".format(input_file_name,df.shape[0]))
    logger.info("Saving file {} as csv".format(input_file_name))
    df.to_csv("{}/{}".format(local_dir,output_file_name),index=False)
    logger.info("Done")

@send_alert(test = test)
def get_data_from_local(logger, file_name, **kwargs):
    ti = kwargs['ti']    

    #Get the list of columns and rename them to Upper Case.
    df = pd.read_csv(f"{local_dir}/{file_name}")
    for c in df.columns:
        df.rename(columns = {c:c.upper()}, inplace=True)
    columns = df.columns
    xcom_data = {"file_name": file_name, 'columns': columns}
    ti.xcom_push(key = 'file_and_columns', value = xcom_data)

@send_alert(test = test)
def load_to_s3(logger, file_name, ds_nodash, **kwargs):
    ti = kwargs['ti']    
    s3 = boto3.resource('s3', aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key)
    #set YYYYMM for bucket
    datef = file_name.replace("_"," ").replace("."," ")
    datef = [int(s) for s in datef.split() if s.isdigit()]
    yyyymm = str(datef[0])[:-2]

    bucket = 'STARTAPP-data-lake'; key = f'Airflow/JBREC/{yyyymm}/{file_name}'
    logger.info(f"Uploading {file_name} from Airflow local TO S3 location s3://{bucket}/{key}")
    s3.Bucket(bucket).upload_file(f"{local_dir}/{file_name}", key) #send to updated weekly files bucket
 
@send_alert( test = test)
def s3_to_snowflake(logger, file_name, ds_nodash, **kwargs):
    dict = ast.literal_eval(Variable.get('DEMO_RAW_TABLE_NAMES') )
    ti = kwargs['ti']
    print(type(dict))
    print(dict.keys())
    #Get the file name and the columns.
    xcom_data = ti.xcom_pull(task_ids = f"get_data_from_local-{file_name.split('2')[0][:-1]}", key = 'file_and_columns')
    columns = xcom_data['columns']
    #Prepare the columns for table creation.
    sf_columns = ''
    for column_name in columns:
        sf_columns = sf_columns+f' "{column_name}" STRING,'
    sf_columns = sf_columns.rstrip(',')        
    schema = "RAWDATA"
    #table = dict[filename] for filename in dict.keys() if filename = file_name

    #set YYYYMM for bucket
    datef = file_name.replace("_"," ").replace("."," ")
    datef = [int(s) for s in datef.split() if s.isdigit()]
    yyyymm = str(datef[0])[:-2]

    for filename in dict.keys():
        if filename == file_name.split('2')[0]:
            print(dict[filename])
            table = f'{dict[filename]}_{yyyymm}'

    bucket = 'STARTAPP-data-lake'; key = f'Airflow/JBREC/{yyyymm}'
    conn = snowflake_hook.get_cursor()
    try:
        create_stage_query = f"""CREATE OR REPLACE STAGE {schema}.RAW_JBREC 
                        url = 's3://{bucket}/{key}'
                        credentials=(aws_key_id= '{aws_access_key_id}', aws_secret_key= '{aws_secret_access_key}')
                        file_format=(type=csv skip_header=1 NULL_IF='' FIELD_OPTIONALLY_ENCLOSED_BY='"')"""

        logger.info(create_stage_query)
        conn.execute(create_stage_query)  
        logger.info(f"Copying data from file {file_name} to Snowflake table: {table}")
        create_table_query = f'CREATE OR REPLACE TABLE {schema}.{table}({sf_columns})'
        logger.info(create_table_query)
        conn.execute(create_table_query)
        copy_query = f'COPY INTO {schema}.{table} FROM @{schema}.RAW_JBREC/{file_name} ON_ERROR = "SKIP_FILE_0.01%" '
        logger.info(copy_query)
        conn.execute(copy_query)
        upload_query_result = conn.fetchone()
        logger.info('upload data query result: {}'.format(upload_query_result))
        if (upload_query_result[1] == 'LOAD_FAILED'):
            raise AirflowException("copy failed.")
        logger.info("Data copied to Snowflake successfully.")
    except Exception as e:
        raise AirflowException(e)    
    ti.xcom_push(key = "table_name", value = table) 


default_args = dict(
    owner = dag_owner,
    start_date = datetime(2020, 10, 14))

dag = DAG(dag_name,
        default_args = default_args,
        catchup = False,
        schedule_interval = "@once") 

dummy = DummyOperator(task_id = "dummy", dag = dag)

task1a = ShortCircuitOperator( dag=dag,
                        task_id='look_for_new_feeds',
                        python_callable=look_for_new_feeds,
                        provide_context=True
                        )

task1b = PythonOperator( dag = dag,
                        task_id = f"download_new_feed",
                        python_callable = download_new_feed,
                        provide_context = True,
                        #op_kwargs = {"file_name" : file}
                        )    

dummy >> task1a >> task1b >> dummy


files_xlsx = os.listdir(input_files_path)
print(files_xlsx)
for input_file_name in files_xlsx:                
    
    task1c = PythonOperator( dag = dag,
                        task_id = f"excel_to_csv-{input_file_name.split('2')[0][:-1]}",
                        python_callable = process_df,
                        provide_context = True,
                        op_kwargs = {"input_file_name" : input_file_name}
                        )

    dummy >> task1c >> dummy

files = os.listdir(local_dir)

for file in files:
    task1 = PythonOperator( dag = dag,
                        task_id = f"get_data_from_local-{file.split('2')[0][:-1]}",
                        python_callable = get_data_from_local,
                        provide_context = True,
                        op_kwargs = {"file_name" : file}
                        )
    task2 = PythonOperator( dag = dag,
                        task_id = f"load_to_s3-{file.split('2')[0][:-1]}",
                        python_callable = load_to_s3,
                        provide_context = True,
                        op_kwargs = {"file_name" : file}
                        )
    task3 = PythonOperator( dag = dag,
                        task_id = f"s3_to_snowflake-{file.split('2')[0][:-1]}",
                        python_callable = s3_to_snowflake,
                        provide_context = True,
                        op_kwargs = {"file_name" : file}
                        )

    dummy >> task1 >> task2 >> task3