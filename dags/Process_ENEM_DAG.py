from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from datetime import datetime

AWS_ACCOUNT_INFO = "123456789000-eu-central-1"

default_args = {
    'owner': 'vjinsi',
    'start_date': datetime(2022, 10, 29),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}
dag = DAG(
    'ProcessENEMdata',
    default_args=default_args,
    description='Uploads ENEM files to S3, replaces values and saves as Parquet to run Crawler on',
    template_searchpath="/home/victor/Documents/Spark_Project_INEP/Airflow/"
    #schedule_interval=timedelta(days=1),
)

# =======================================================================================================
#                                       WGET RAW DATA
# =======================================================================================================

year = "2004"
upload_ENEM_2004 = BashOperator(
    task_id="upload_ENEM_2004",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

year = "2005"
upload_ENEM_2005 = BashOperator(
    task_id="upload_ENEM_2005",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

year = "2006"
upload_ENEM_2006 = BashOperator(
    task_id="upload_ENEM_2006",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

year = "2007"
upload_ENEM_2007 = BashOperator(
    task_id="upload_ENEM_2007",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

year = "2008"
upload_ENEM_2008 = BashOperator(
    task_id="upload_ENEM_2008",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

year = "2009"
upload_ENEM_2009 = BashOperator(
    task_id="upload_ENEM_2009",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

year = "2010"
upload_ENEM_2010 = BashOperator(
    task_id="upload_ENEM_2010",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

year = "2011"
upload_ENEM_2011 = BashOperator(
    task_id="upload_ENEM_2011",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

# 2012 file corrupted

year = "2013"
upload_ENEM_2013 = BashOperator(
    task_id="upload_ENEM_2013",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

year = "2014"
upload_ENEM_2014 = BashOperator(
    task_id="upload_ENEM_2014",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

year = "2015"
upload_ENEM_2015 = BashOperator(
    task_id="upload_ENEM_2015",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

year = "2016"
upload_ENEM_2016 = BashOperator(
    task_id="upload_ENEM_2016",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

year = "2017"
upload_ENEM_2017 = BashOperator(
    task_id="upload_ENEM_2017",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

year = "2018"
upload_ENEM_2018 = BashOperator(
    task_id="upload_ENEM_2018",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

year = "2019"
upload_ENEM_2019 = BashOperator(
    task_id="upload_ENEM_2019",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

year = "2020"
upload_ENEM_2020 = BashOperator(
    task_id="upload_ENEM_2020",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

year = "2021"
upload_ENEM_2021 = BashOperator(
    task_id="upload_ENEM_2021",
    bash_command="upload_ENEM_s3.sh",
    params = {"link": "https://download.inep.gov.br/microdados/microdados_enem_" + year + ".zip", "year": year},
    dag=dag
)

# =======================================================================================================
#                                           GLUE JOBS (ETL)
# =======================================================================================================

job_name="Replace values ENEM"

year = "2004"
glue_enem_2004 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2004 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2004.output,
    dag=dag
)

year = "2005"
glue_enem_2005 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2005 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2005.output,
    dag=dag
)

year = "2006"
glue_enem_2006 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2006 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2006.output,
    dag=dag
)

year = "2007"
glue_enem_2007 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2007 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2007.output,
    dag=dag
)

year = "2008"
glue_enem_2008 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2008 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2008.output,
    dag=dag
)

year = "2009"
glue_enem_2009 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2009 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2009.output,
    dag=dag
)

year = "2010"
glue_enem_2010 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2010 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2010.output,
    dag=dag
)

year = "2011"
glue_enem_2011 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2011 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2011.output,
    dag=dag
)

# 2012 file corrupted

year = "2013"
glue_enem_2013 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2013 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2013.output,
    dag=dag
)

year = "2014"
glue_enem_2014 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2014 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2014.output,
    dag=dag
)

year = "2015"
glue_enem_2015 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2015 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2015.output,
    dag=dag
)

year = "2016"
glue_enem_2016 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2016 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2016.output,
    dag=dag
)

year = "2017"
glue_enem_2017 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2017 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2017.output,
    dag=dag
)

year = "2018"
glue_enem_2018 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2018 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2018.output,
    dag=dag
)

year = "2019"
glue_enem_2019 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2019 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2019.output,
    dag=dag
)

year = "2020"
glue_enem_2020 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2020 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2020.output,
    dag=dag
)

year = "2021"
glue_enem_2021 = GlueJobOperator(
    task_id='Replace_Values_ENEM_' + year,
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Replace values ENEM.py",
    script_args={"--YEAR": int(year)},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_enem_2021 = GlueJobSensor(
    task_id='Wait_for_Replace_Values_ENEM_' + year,
    job_name=job_name,
    run_id=glue_enem_2021.output,
    dag=dag
)

# =======================================================================================================
#                                       ORDER TASKS IN DAG
# =======================================================================================================

upload_ENEM_2004 >> glue_enem_2004 >> wait_glue_enem_2004
upload_ENEM_2005 >> glue_enem_2005 >> wait_glue_enem_2005
upload_ENEM_2006 >> glue_enem_2006 >> wait_glue_enem_2006
upload_ENEM_2007 >> glue_enem_2007 >> wait_glue_enem_2007
upload_ENEM_2008 >> glue_enem_2008 >> wait_glue_enem_2008
upload_ENEM_2009 >> glue_enem_2009 >> wait_glue_enem_2009
upload_ENEM_2010 >> glue_enem_2010 >> wait_glue_enem_2010
upload_ENEM_2011 >> glue_enem_2011 >> wait_glue_enem_2011
upload_ENEM_2013 >> glue_enem_2013 >> wait_glue_enem_2013
upload_ENEM_2014 >> glue_enem_2014 >> wait_glue_enem_2014
upload_ENEM_2015 >> glue_enem_2015 >> wait_glue_enem_2015
upload_ENEM_2016 >> glue_enem_2016 >> wait_glue_enem_2016
upload_ENEM_2017 >> glue_enem_2017 >> wait_glue_enem_2017
upload_ENEM_2018 >> glue_enem_2018 >> wait_glue_enem_2018
upload_ENEM_2019 >> glue_enem_2019 >> wait_glue_enem_2019
upload_ENEM_2020 >> glue_enem_2020 >> wait_glue_enem_2020
upload_ENEM_2021 >> glue_enem_2021 >> wait_glue_enem_2021

# to avoid concurrent run error in Glue

wait_glue_enem_2004 >> glue_enem_2005
wait_glue_enem_2005 >> glue_enem_2006
wait_glue_enem_2006 >> glue_enem_2007
wait_glue_enem_2007 >> glue_enem_2008
wait_glue_enem_2008 >> glue_enem_2009
wait_glue_enem_2009 >> glue_enem_2010
wait_glue_enem_2010 >> glue_enem_2011
wait_glue_enem_2013 >> glue_enem_2014
wait_glue_enem_2014 >> glue_enem_2015
wait_glue_enem_2015 >> glue_enem_2016
wait_glue_enem_2016 >> glue_enem_2017
wait_glue_enem_2017 >> glue_enem_2018
wait_glue_enem_2018 >> glue_enem_2019
wait_glue_enem_2019 >> glue_enem_2020
wait_glue_enem_2020 >> glue_enem_2021