from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from datetime import datetime

default_args = {
    'owner': 'vjinsi',
    'start_date': datetime(2022, 10, 28),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}
dag = DAG(
    'ProcessPISAdata',
    default_args=default_args,
    description='Uploads PISA files to S3, replaces values and saves as Parquet to run Crawler on',
    template_searchpath="/home/victor/Documents/Spark_Project_INEP/Airflow/"
    #schedule_interval=timedelta(days=1),
)

PISA_DATA_BUCKET = "inep_data"
AWS_ACCOUNT_INFO = "123456789000-eu-central-1"

# =======================================================================================================
#                                       WGET RAW DATA
# =======================================================================================================

# ======================================  YEAR 2000 =====================================================

links_2000 = '"https://www.oecd.org/pisa/pisaproducts/intscho.zip https://www.oecd.org/pisa/pisaproducts/intstud_math.zip https://www.oecd.org/pisa/pisaproducts/intstud_read.zip https://www.oecd.org/pisa/pisaproducts/intstud_scie.zip" "https://www.oecd.org/pisa/pisaproducts/PISA2000_SAS_school_questionnaire.sas" "https://www.oecd.org/pisa/pisaproducts/PISA2000_SAS_student_mathematics.sas" "https://www.oecd.org/pisa/pisaproducts/PISA2000_SAS_student_reading.sas" "https://www.oecd.org/pisa/pisaproducts/PISA2000_SAS_student_science.sas"'
upload_PISA_2000 = BashOperator(
    task_id="upload_PISA_2000",
    bash_command="upload_PISA_s3.sh",
    params = {"links": links_2000, "year": 2000},
    dag=dag
)

# ======================================  YEAR 2003 =====================================================

links_2003 = '"https://www.oecd.org/pisa/pisaproducts/INT_schi_2003.zip https://www.oecd.org/pisa/pisaproducts/PISA2003_SAS_school.sas https://www.oecd.org/pisa/pisaproducts/INT_stui_2003_v2.zip https://www.oecd.org/pisa/pisaproducts/PISA2003_SAS_student.sas"'

upload_PISA_2003 = BashOperator(
    task_id="upload_PISA_2003",
    bash_command="upload_PISA_s3.sh",
    params = {"links": links_2003, "year": 2003},
    dag=dag
)

# ======================================  YEAR 2006 =====================================================

links_2006 = '"https://www.oecd.org/pisa/pisaproducts/INT_Stu06_Dec07.zip https://www.oecd.org/pisa/pisaproducts/INT_Sch06_Dec07.zip https://www.oecd.org/pisa/pisaproducts/INT_Par06_Dec07.zip https://www.oecd.org/pisa/pisaproducts/PISA2006_SAS_student.sas https://www.oecd.org/pisa/pisaproducts/PISA2006_SAS_school.sas https://www.oecd.org/pisa/pisaproducts/PISA2006_SAS_parent.sas"'

upload_PISA_2006 = BashOperator(
    task_id="upload_PISA_2006",
    bash_command="upload_PISA_s3.sh",
    params = {"links": links_2006, "year": 2006},
    dag=dag
)

# ======================================  YEAR 2009 =====================================================

links_2009 = '"https://www.oecd.org/pisa/pisaproducts/PISA2009_SAS_student.sas https://www.oecd.org/pisa/pisaproducts/PISA2009_SAS_school.sas https://www.oecd.org/pisa/pisaproducts/PISA2009_SAS_parent.sas https://www.oecd.org/pisa/pisaproducts/INT_STQ09_DEC11.zip https://www.oecd.org/pisa/pisaproducts/INT_SCQ09_Dec11.zip https://www.oecd.org/pisa/pisaproducts/INT_PAR09_DEC11.zip"'

upload_PISA_2009 = BashOperator(
    task_id="upload_PISA_2009",
    bash_command="upload_PISA_s3.sh",
    params = {"links": links_2009, "year": 2009},
    dag=dag
)

# ======================================  YEAR 2012 =====================================================

links_2012 = '"https://www.oecd.org/pisa/pisaproducts/PISA2012_SAS_student.sas https://www.oecd.org/pisa/pisaproducts/PISA2012_SAS_school.sas https://www.oecd.org/pisa/pisaproducts/PISA2012_SAS_parent.sas https://www.oecd.org/pisa/pisaproducts/INT_STU12_DEC03.zip https://www.oecd.org/pisa/pisaproducts/INT_SCQ12_DEC03.zip https://www.oecd.org/pisa/pisaproducts/INT_PAQ12_DEC03.zip"'

upload_PISA_2012 = BashOperator(
    task_id="upload_PISA_2012",
    bash_command="upload_PISA_s3.sh",
    params = {"links": links_2012, "year": 2012},
    dag=dag
)

# =======================================================================================================
#                                   SAVE AS CSV TO S3
# =======================================================================================================

# ======================================  YEAR 2000 =====================================================

lambda_student_2000_math = AwsLambdaInvokeFunctionOperator(
    task_id="Unzip_and_to_CSV_Student_2000_math",
    function_name="Unzip_and_to_CSV",
    payload=b'{"sas_file": "PISA 2000/PISA2000_SAS_student_mathematics.sas", "in_path": "PISA 2000/intstud_math", "out_path": "PISA 2000/Data Math/intstud_math.csv", "limits": "846,1169"}',
    dag=dag
)

lambda_student_2000_read = AwsLambdaInvokeFunctionOperator(
    task_id="Unzip_and_to_CSV_Student_2000_read",
    function_name="Unzip_and_to_CSV",
    payload=b'{"sas_file": "PISA 2000/PISA2000_SAS_student_reading.sas", "in_path": "PISA 2000/intstud_read", "out_path": "PISA 2000/Data Read/intstud_read.csv", "limits": "830,1146"}',
    dag=dag
)

lambda_school_2000 = AwsLambdaInvokeFunctionOperator(
    task_id="Unzip_and_to_CSV_School_2000",
    function_name="Unzip_and_to_CSV",
    payload=b'{"sas_file": "PISA 2000/PISA2000_SAS_school_questionnaire.sas", "in_path": "PISA 2000/intscho", "out_path": "PISA 2000/Data School/intscho.csv", "limits": "400,568"}',
    dag=dag
)

lambda_student_2000_science = AwsLambdaInvokeFunctionOperator(
    task_id="Unzip_and_to_CSV_Student_2000_science",
    function_name="Unzip_and_to_CSV",
    payload=b'{"sas_file": "PISA 2000/PISA2000_SAS_student_science.sas", "in_path": "PISA 2000/intstud_scie", "out_path": "PISA 2000/Data Scie/intstud_scie.csv", "limits": "834,1157"}',
    dag=dag
)

# ======================================  YEAR 2003 =====================================================

lambda_student_2003 = AwsLambdaInvokeFunctionOperator(
    task_id="Unzip_and_to_CSV_Student_2003",
    function_name="Unzip_and_to_CSV",
    payload=b'{"sas_file": "PISA 2003/PISA2003_SAS_student.sas", "in_path": "PISA 2000/INT_stui_2003_v2", "out_path": "PISA 2003/Data/INT_stui_2003_v2.csv", "limits": "2232,2635"}',
    dag=dag
)

lambda_school_2003 = AwsLambdaInvokeFunctionOperator(
    task_id="Unzip_and_to_CSV_School_2003",
    function_name="Unzip_and_to_CSV",
    payload=b'{"sas_file": "PISA 2003/PISA2003_SAS_school.sas", "in_path": "PISA 2003/INT_schi_2003", "out_path": "PISA 2003/Data School/INT_schi_2003.csv", "limits": "948,1137"}',
    dag=dag
)

# ======================================  YEAR 2006 =====================================================

lambda_student_2006 = AwsLambdaInvokeFunctionOperator(
    task_id="Unzip_and_to_CSV_Student_2006",
    function_name="Unzip_and_to_CSV",
    payload=b'{"sas_file": "PISA 2006/PISA2006_SAS_student.sas", "in_path": "PISA 2006/INT_Stud06_Dec07", "out_path": "PISA 2006/Data/INT_Stud06_Dec07.csv", "limits": "3622,4111"}',
    dag=dag
)

lambda_school_2006 = AwsLambdaInvokeFunctionOperator(
    task_id="Unzip_and_to_CSV_School_2006",
    function_name="Unzip_and_to_CSV",
    payload=b'{"sas_file": "PISA 2006/PISA2006_SAS_school.sas", "in_path": "PISA 2006/INT_Sch06_Dec07", "out_path": "PISA 2006/Data School/INT_Sch06_Dec07.csv", "limits": "1508,1694"}',
    dag=dag
)

lambda_parent_2006 = AwsLambdaInvokeFunctionOperator(
    task_id="Unzip_and_to_CSV_Parent_2006",
    function_name="Unzip_and_to_CSV",
    payload=b'{"sas_file": "PISA 2006/PISA2006_SAS_parent.sas", "in_path": "PISA 2006/INT_Par06_Dec07", "out_path": "PISA 2006/Data Parent/INT_Par06_Dec07.csv", "limits": "1142,1225"}',
    dag=dag
)

# ======================================  YEAR 2009 =====================================================

lambda_student_2009 = AwsLambdaInvokeFunctionOperator(
    task_id="Unzip_and_to_CSV_Student_2009",
    function_name="Unzip_and_to_CSV",
    payload=b'{"sas_file": "PISA 2009/PISA2009_SAS_student.sas", "in_path": "PISA 2009/INT_STQ09_DEC11", "out_path": "PISA 2009/Data/INT_STQ09_DEC11.csv", "limits": "4406,4842"}',
    dag=dag
)

lambda_school_2009 = AwsLambdaInvokeFunctionOperator(
    task_id="Unzip_and_to_CSV_School_2009",
    function_name="Unzip_and_to_CSV",
    payload=b'{"sas_file": "PISA 2009/PISA2009_SAS_school.sas", "in_path": "PISA 2009/INT_SCQ09_Dec11", "out_path": "PISA 2009/Data School/INT_SCQ09_Dec11.csv", "limits": "2561,2807"}',
    dag=dag
)

lambda_parent_2009 = AwsLambdaInvokeFunctionOperator(
    task_id="Unzip_and_to_CSV_Parent_2009",
    function_name="Unzip_and_to_CSV",
    payload=b'{"sas_file": "PISA 2009/PISA2009_SAS_parent.sas", "in_path": "PISA 2009/INT_PAR09_DEC11", "out_path": "PISA 2009/Data Parent/INT_PAR09_DEC11.csv", "limits": "2168,2257"}',
    dag=dag
)

# ======================================  YEAR 2012 =====================================================

lambda_student_2012 = AwsLambdaInvokeFunctionOperator(
    task_id="Unzip_and_to_CSV_Student_2012",
    function_name="Unzip_and_to_CSV",
    payload=b'{"sas_file": "PISA 2012/PISA2012_SAS_student.sas", "in_path": "PISA 2012/INT_STU12_DEC03", "out_path": "PISA 2012/Data/INT_STU12_DEC03.csv", "limits": "8991,9624"}',
    dag=dag
)

lambda_school_2012 = AwsLambdaInvokeFunctionOperator(
    task_id="Unzip_and_to_CSV_School_2012",
    function_name="Unzip_and_to_CSV",
    payload=b'{"sas_file": "PISA 2012/PISA2012_SAS_school.sas", "in_path": "PISA 2012/INT_SCQ12_DEC03", "out_path": "PISA 2012/Data/INT_SCQ12_DEC03.csv", "limits": "4599,4889"}',
    dag=dag
)

lambda_parent_2012 = AwsLambdaInvokeFunctionOperator(
    task_id="Unzip_and_to_CSV_Parent_2012",
    function_name="Unzip_and_to_CSV",
    payload=b'{"sas_file": "PISA 2012/PISA2012_SAS_parent.sas", "in_path": "PISA 2012/INT_PAQ12_DEC03", "out_path": "PISA 2012/Data/INT_PAQ12_DEC03.csv", "limits": "4485,4627"}',
    dag=dag
)

# =======================================================================================================
#                                           GLUE JOBS (ETL)
# =======================================================================================================

# ======================================  YEAR 2000, Mathematics ========================================

job_name="Rename Columns INEP Student"
glue_student_2000_math_rename = GlueJobOperator(
    task_id='Rename_Columns_INEP_Student_2000_Math',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Rename Columns INEP Student.py",
    script_args={"--CSV": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/Data Math", "--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/math_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/PISA2000_SAS_student_mathematics.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2000_math_rename = GlueJobSensor(
    task_id='Wait_for_Rename_Columns_INEP_Student_2000_Math',
    job_name=job_name,
    run_id=glue_student_2000_math_rename.output,
    dag=dag
)

job_name="INEP Student Data ETL Replace Fields"
glue_student_2000_math_replace = GlueJobOperator(
    task_id='Replace_Fields_Student_2000_Math',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/INEP Student Data ETL Replace Fields.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/math_replaced.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/math_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/PISA2000_SAS_student_mathematics.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2000_math_replace = GlueJobSensor(
    task_id='Wait_for_Replace_Fields_Student_2000_Math',
    job_name=job_name,
    run_id=glue_student_2000_math_replace.output,
    dag=dag
)

job_name="Second Rename Columns INEP Student"
glue_student_2000_math_rename_second = GlueJobOperator(
    task_id='Second_Rename_Student_2000_Math',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Second Rename Columns INEP Student.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/math_out.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/math_replaced.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/PISA2000_SAS_student_mathematics.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2000_math_rename_second = GlueJobSensor(
    task_id='Wait_for_Second_Rename_Student_2000_Math',
    job_name=job_name,
    run_id=glue_student_2000_math_rename_second.output,
    dag=dag
)

# ======================================  YEAR 2000, Reading ========================================

job_name="Rename Columns INEP Student"
glue_student_2000_read_rename = GlueJobOperator(
    task_id='Rename_Columns_INEP_Student_2000_Read',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Rename Columns INEP Student.py",
    script_args={"--CSV": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/Data Read", "--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/read_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/PISA2000_SAS_student_reading.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2000_read_rename = GlueJobSensor(
    task_id='Wait_for_Rename_Columns_INEP_Student_2000_Read',
    job_name=job_name,
    run_id=glue_student_2000_read_rename.output,
    dag=dag
)

job_name="INEP Student Data ETL Replace Fields"
glue_student_2000_read_replace = GlueJobOperator(
    task_id='Replace_Fields_Student_2000_Read',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/INEP Student Data ETL Replace Fields.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/read_replaced.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/read_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/PISA2000_SAS_student_reading.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2000_read_replace = GlueJobSensor(
    task_id='Wait_for_Replace_Fields_Student_2000_Read',
    job_name=job_name,
    run_id=glue_student_2000_read_replace.output,
    dag=dag
)

job_name="Second Rename Columns INEP Student"
glue_student_2000_read_rename_second = GlueJobOperator(
    task_id='Second_Rename_Student_2000_Read',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Second Rename Columns INEP Student.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/read_out.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/read_replaced.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/PISA2000_SAS_student_reading.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2000_read_rename_second = GlueJobSensor(
    task_id='Wait_for_Second_Rename_Student_2000_Read',
    job_name=job_name,
    run_id=glue_student_2000_read_rename_second.output,
    dag=dag
)

# ======================================  YEAR 2000, Science ========================================

job_name="Rename Columns INEP Student"
glue_student_2000_science_rename = GlueJobOperator(
    task_id='Rename_Columns_INEP_Student_2000_Science',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Rename Columns INEP Student.py",
    script_args={"--CSV": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/Data Scie", "--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/scie_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/PISA2000_SAS_student_science.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2000_science_rename = GlueJobSensor(
    task_id='Wait_for_Rename_Columns_INEP_Student_2000_Science',
    job_name=job_name,
    run_id=glue_student_2000_science_rename.output,
    dag=dag
)

job_name="INEP Student Data ETL Replace Fields"
glue_student_2000_science_replace = GlueJobOperator(
    task_id='Replace_Fields_Student_2000_Science',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/INEP Student Data ETL Replace Fields.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/scie_replaced.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/scie_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/PISA2000_SAS_student_science.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2000_science_replace = GlueJobSensor(
    task_id='Wait_for_Replace_Fields_Student_2000_Science',
    job_name=job_name,
    run_id=glue_student_2000_science_replace.output,
    dag=dag
)

job_name="Second Rename Columns INEP Student"
glue_student_2000_science_rename_second = GlueJobOperator(
    task_id='Second_Rename_Student_2000_Science',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Second Rename Columns INEP Student.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/scie_out.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/scie_replaced.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/PISA2000_SAS_student_science.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2000_science_rename_second = GlueJobSensor(
    task_id='Wait_for_Second_Rename_Student_2000_Science',
    job_name=job_name,
    run_id=glue_student_2000_science_rename_second.output,
    dag=dag
)

# ======================================  YEAR 2000, School ========================================

job_name="Rename Columns INEP Student"
glue_school_2000_rename = GlueJobOperator(
    task_id='Rename_Columns_INEP_School_2000',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Rename Columns INEP Student.py",
    script_args={"--CSV": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/Data School", "--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/school_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/PISA2000_SAS_school_questionnaire.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_school_2000_rename = GlueJobSensor(
    task_id='Wait_for_Rename_Columns_INEP_School_2000',
    job_name=job_name,
    run_id=glue_school_2000_rename.output,
    dag=dag
)

job_name="INEP Student Data ETL Replace Fields"
glue_school_2000_replace = GlueJobOperator(
    task_id='Replace_Fields_School_2000',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/INEP Student Data ETL Replace Fields.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/school_replaced.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/school_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/PISA2000_SAS_school_questionnaire.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_school_2000_replace = GlueJobSensor(
    task_id='Wait_for_Replace_Fields_School_2000',
    job_name=job_name,
    run_id=glue_school_2000_replace.output,
    dag=dag
)

job_name="Second Rename Columns INEP Student"
glue_school_2000_rename_second = GlueJobOperator(
    task_id='Second_Rename_School_2000',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Second Rename Columns INEP Student.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/school_out.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/school_replaced.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2000/PISA2000_SAS_school_questionnaire.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_school_2000_rename_second = GlueJobSensor(
    task_id='Wait_for_Second_Rename_School_2000',
    job_name=job_name,
    run_id=glue_school_2000_rename_second.output,
    dag=dag
)

# ======================================  YEAR 2003, Student ========================================

job_name="Rename Columns INEP Student"
glue_student_2003_rename = GlueJobOperator(
    task_id='Rename_Columns_INEP_Student_2003',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Rename Columns INEP Student.py",
    script_args={"--CSV": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/Data", "--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/student_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/PISA2003_SAS_student.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2003_rename = GlueJobSensor(
    task_id='Wait_for_Rename_Columns_INEP_Student_2003',
    job_name=job_name,
    run_id=glue_student_2003_rename.output,
    dag=dag
)

job_name="INEP Student Data ETL Replace Fields"
glue_student_2003_replace = GlueJobOperator(
    task_id='Replace_Fields_Student_2003',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/INEP Student Data ETL Replace Fields.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/student_replaced.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/student_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/PISA2003_SAS_student.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2003_replace = GlueJobSensor(
    task_id='Wait_for_Replace_Fields_Student_2003',
    job_name=job_name,
    run_id=glue_student_2003_replace.output,
    dag=dag
)

job_name="Second Rename Columns INEP Student"
glue_student_2003_rename_second = GlueJobOperator(
    task_id='Second_Rename_Student_2003',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Second Rename Columns INEP Student.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/student_out.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/student_replaced.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/PISA2003_SAS_student.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" + AWS_ACCOUNT_INFO + "",
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2003_rename_second = GlueJobSensor(
    task_id='Wait_for_Second_Rename_Student_2003',
    job_name=job_name,
    run_id=glue_student_2003_rename_second.output,
    dag=dag
)

# ======================================  YEAR 2003, School ========================================

job_name="Rename Columns INEP Student"
glue_school_2003_rename = GlueJobOperator(
    task_id='Rename_Columns_INEP_School_2003',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Rename Columns INEP Student.py",
    script_args={"--CSV": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/Data School", "--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/school_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/PISA2003_SAS_school.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_school_2003_rename = GlueJobSensor(
    task_id='Wait_for_Rename_Columns_INEP_School_2003',
    job_name=job_name,
    run_id=glue_school_2003_rename.output,
    dag=dag
)

job_name="INEP Student Data ETL Replace Fields"
glue_school_2003_replace = GlueJobOperator(
    task_id='Replace_Fields_School_2003',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/INEP Student Data ETL Replace Fields.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/school_replaced.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/school_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/PISA2003_SAS_school.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_school_2003_replace = GlueJobSensor(
    task_id='Wait_for_Replace_Fields_School_2003',
    job_name=job_name,
    run_id=glue_school_2003_replace.output,
    dag=dag
)

job_name="Second Rename Columns INEP Student"
glue_school_2003_rename_second = GlueJobOperator(
    task_id='Second_Rename_School_2003',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Second Rename Columns INEP Student.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/school_out.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/school_replaced.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2003/PISA2003_SAS_school.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_school_2003_rename_second = GlueJobSensor(
    task_id='Wait_for_Second_Rename_School_2003',
    job_name=job_name,
    run_id=glue_school_2003_rename_second.output,
    dag=dag
)

# ======================================  YEAR 2006, Student ========================================

job_name="Rename Columns INEP Student"
glue_student_2006_rename = GlueJobOperator(
    task_id='Rename_Columns_INEP_Student_2006',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Rename Columns INEP Student.py",
    script_args={"--CSV": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/Data", "--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/student_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/PISA2006_SAS_student.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2006_rename = GlueJobSensor(
    task_id='Wait_for_Rename_Columns_INEP_Student_2006',
    job_name=job_name,
    run_id=glue_student_2006_rename.output,
    dag=dag
)

job_name="INEP Student Data ETL Replace Fields"
glue_student_2006_replace = GlueJobOperator(
    task_id='Replace_Fields_Student_2006',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/INEP Student Data ETL Replace Fields.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/student_replaced.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/student_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/PISA2006_SAS_student.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2006_replace = GlueJobSensor(
    task_id='Wait_for_Replace_Fields_Student_2006',
    job_name=job_name,
    run_id=glue_student_2006_replace.output,
    dag=dag
)

job_name="Second Rename Columns INEP Student"
glue_student_2006_rename_second = GlueJobOperator(
    task_id='Second_Rename_Student_2006',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Second Rename Columns INEP Student.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/student_out.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/student_replaced.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/PISA2006_SAS_student.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2006_rename_second = GlueJobSensor(
    task_id='Wait_for_Second_Rename_Student_2006',
    job_name=job_name,
    run_id=glue_student_2006_rename_second.output,
    dag=dag
)

# ======================================  YEAR 2006, School ========================================

job_name="Rename Columns INEP Student"
glue_school_2006_rename = GlueJobOperator(
    task_id='Rename_Columns_INEP_School_2006',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Rename Columns INEP Student.py",
    script_args={"--CSV": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/Data School", "--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/school_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/PISA2006_SAS_school.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_school_2006_rename = GlueJobSensor(
    task_id='Wait_for_Rename_Columns_INEP_School_2006',
    job_name=job_name,
    run_id=glue_school_2006_rename.output,
    dag=dag
)

job_name="INEP Student Data ETL Replace Fields"
glue_school_2006_replace = GlueJobOperator(
    task_id='Replace_Fields_School_2006',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/INEP Student Data ETL Replace Fields.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/school_replaced.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/school_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/PISA2006_SAS_school.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_school_2006_replace = GlueJobSensor(
    task_id='Wait_for_Replace_Fields_School_2006',
    job_name=job_name,
    run_id=glue_school_2006_replace.output,
    dag=dag
)

job_name="Second Rename Columns INEP Student"
glue_school_2006_rename_second = GlueJobOperator(
    task_id='Second_Rename_School_2006',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Second Rename Columns INEP Student.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/school_out.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/school_replaced.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/PISA2006_SAS_school.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_school_2006_rename_second = GlueJobSensor(
    task_id='Wait_for_Second_Rename_School_2006',
    job_name=job_name,
    run_id=glue_school_2006_rename_second.output,
    dag=dag
)

# ======================================  YEAR 2006, Parent ========================================

job_name="Rename Columns INEP Student"
glue_parent_2006_rename = GlueJobOperator(
    task_id='Rename_Columns_INEP_Parent_2006',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Rename Columns INEP Student.py",
    script_args={"--CSV": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/Data Parent", "--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/parent_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/PISA2006_SAS_parent.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_parent_2006_rename = GlueJobSensor(
    task_id='Wait_for_Rename_Columns_INEP_Parent_2006',
    job_name=job_name,
    run_id=glue_parent_2006_rename.output,
    dag=dag
)

job_name="INEP Student Data ETL Replace Fields"
glue_parent_2006_replace = GlueJobOperator(
    task_id='Replace_Fields_Parent_2006',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/INEP Student Data ETL Replace Fields.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/parent_replaced.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/parent_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/PISA2006_SAS_parent.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_parent_2006_replace = GlueJobSensor(
    task_id='Wait_for_Replace_Fields_Parent_2006',
    job_name=job_name,
    run_id=glue_parent_2006_replace.output,
    dag=dag
)

job_name="Second Rename Columns INEP Parent"
glue_parent_2006_rename_second = GlueJobOperator(
    task_id='Second_Rename_Parent_2006',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Second Rename Columns INEP Student.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/parent_out.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/parent_replaced.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/PISA2006_SAS_parent.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_parent_2006_rename_second = GlueJobSensor(
    task_id='Wait_for_Second_Rename_Parent_2006',
    job_name=job_name,
    run_id=glue_parent_2006_rename_second.output,
    dag=dag
)

# ======================================  YEAR 2009, Student ========================================

job_name="Rename Columns INEP Student"
glue_student_2009_rename = GlueJobOperator(
    task_id='Rename_Columns_INEP_Student_2009',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Rename Columns INEP Student.py",
    script_args={"--CSV": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/Data", "--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/student_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/PISA2009_SAS_student.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2009_rename = GlueJobSensor(
    task_id='Wait_for_Rename_Columns_INEP_Student_2009',
    job_name=job_name,
    run_id=glue_student_2009_rename.output,
    dag=dag
)

job_name="INEP Student Data ETL Replace Fields"
glue_student_2009_replace = GlueJobOperator(
    task_id='Replace_Fields_Student_2009',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/INEP Student Data ETL Replace Fields.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/student_replaced.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/student_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/PISA2009_SAS_student.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2009_replace = GlueJobSensor(
    task_id='Wait_for_Replace_Fields_Student_2009',
    job_name=job_name,
    run_id=glue_student_2009_replace.output,
    dag=dag
)

job_name="Second Rename Columns INEP Student"
glue_student_2009_rename_second = GlueJobOperator(
    task_id='Second_Rename_Student_2009',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Second Rename Columns INEP Student.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/student_out.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/student_replaced.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/PISA2009_SAS_student.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2009_rename_second = GlueJobSensor(
    task_id='Wait_for_Second_Rename_Student_2009',
    job_name=job_name,
    run_id=glue_student_2009_rename_second.output,
    dag=dag
)

# ======================================  YEAR 2009, School ========================================

job_name="Rename Columns INEP Student"
glue_school_2009_rename = GlueJobOperator(
    task_id='Rename_Columns_INEP_School_2009',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Rename Columns INEP Student.py",
    script_args={"--CSV": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/Data School", "--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/school_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/PISA2009_SAS_school.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_school_2009_rename = GlueJobSensor(
    task_id='Wait_for_Rename_Columns_INEP_School_2009',
    job_name=job_name,
    run_id=glue_school_2009_rename.output,
    dag=dag
)

job_name="INEP Student Data ETL Replace Fields"
glue_school_2009_replace = GlueJobOperator(
    task_id='Replace_Fields_School_2009',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/INEP Student Data ETL Replace Fields.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/school_replaced.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/school_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/PISA2009_SAS_school.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_school_2009_replace = GlueJobSensor(
    task_id='Wait_for_Replace_Fields_School_2009',
    job_name=job_name,
    run_id=glue_school_2009_replace.output,
    dag=dag
)

job_name="Second Rename Columns INEP Student"
glue_school_2009_rename_second = GlueJobOperator(
    task_id='Second_Rename_School_2009',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Second Rename Columns INEP Student.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/school_out.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/school_replaced.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/PISA2009_SAS_school.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_school_2009_rename_second = GlueJobSensor(
    task_id='Wait_for_Second_Rename_School_2009',
    job_name=job_name,
    run_id=glue_school_2009_rename_second.output,
    dag=dag
)

# ======================================  YEAR 2009, Parent ========================================

job_name="Rename Columns INEP Student"
glue_parent_2009_rename = GlueJobOperator(
    task_id='Rename_Columns_INEP_Parent_2009',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Rename Columns INEP Student.py",
    script_args={"--CSV": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/Data Parent", "--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/parent_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/PISA2009_SAS_parent.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_parent_2009_rename = GlueJobSensor(
    task_id='Wait_for_Rename_Columns_INEP_Parent_2009',
    job_name=job_name,
    run_id=glue_parent_2009_rename.output,
    dag=dag
)

job_name="INEP Student Data ETL Replace Fields"
glue_parent_2009_replace = GlueJobOperator(
    task_id='Replace_Fields_Parent_2009',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/INEP Student Data ETL Replace Fields.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/parent_replaced.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/parent_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/PISA2009_SAS_parent.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_parent_2009_replace = GlueJobSensor(
    task_id='Wait_for_Replace_Fields_Parent_2009',
    job_name=job_name,
    run_id=glue_parent_2009_replace.output,
    dag=dag
)

job_name="Second Rename Columns INEP Parent"
glue_parent_2009_rename_second = GlueJobOperator(
    task_id='Second_Rename_Parent_2009',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Second Rename Columns INEP Student.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/parent_out.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/parent_replaced.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2009/PISA2009_SAS_parent.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_parent_2009_rename_second = GlueJobSensor(
    task_id='Wait_for_Second_Rename_Parent_2009',
    job_name=job_name,
    run_id=glue_parent_2009_rename_second.output,
    dag=dag
)

# ======================================  YEAR 2012, Student ========================================

job_name="Rename Columns INEP Student"
glue_student_2012_rename = GlueJobOperator(
    task_id='Rename_Columns_INEP_Student_2012',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Rename Columns INEP Student.py",
    script_args={"--CSV": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/Data", "--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/student_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2006/PISA2012_SAS_student.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2012_rename = GlueJobSensor(
    task_id='Wait_for_Rename_Columns_INEP_Student_2012',
    job_name=job_name,
    run_id=glue_student_2012_rename.output,
    dag=dag
)

job_name="INEP Student Data ETL Replace Fields"
glue_student_2012_replace = GlueJobOperator(
    task_id='Replace_Fields_Student_2012',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/INEP Student Data ETL Replace Fields.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/student_replaced.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/student_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/PISA2012_SAS_student.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2012_replace = GlueJobSensor(
    task_id='Wait_for_Replace_Fields_Student_2012',
    job_name=job_name,
    run_id=glue_student_2012_replace.output,
    dag=dag
)

job_name="Second Rename Columns INEP Student"
glue_student_2012_rename_second = GlueJobOperator(
    task_id='Second_Rename_Student_2012',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Second Rename Columns INEP Student.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/student_out.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/student_replaced.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/PISA2012_SAS_student.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_student_2012_rename_second = GlueJobSensor(
    task_id='Wait_for_Second_Rename_Student_2012',
    job_name=job_name,
    run_id=glue_student_2012_rename_second.output,
    dag=dag
)

# ======================================  YEAR 2012, School ========================================

job_name="Rename Columns INEP Student"
glue_school_2012_rename = GlueJobOperator(
    task_id='Rename_Columns_INEP_School_2012',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Rename Columns INEP Student.py",
    script_args={"--CSV": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/Data School", "--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/school_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/PISA2012_SAS_school.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_school_2012_rename = GlueJobSensor(
    task_id='Wait_for_Rename_Columns_INEP_School_2012',
    job_name=job_name,
    run_id=glue_school_2012_rename.output,
    dag=dag
)

job_name="INEP Student Data ETL Replace Fields"
glue_school_2012_replace = GlueJobOperator(
    task_id='Replace_Fields_School_2012',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/INEP Student Data ETL Replace Fields.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/school_replaced.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/school_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/PISA2012_SAS_school.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_school_2012_replace = GlueJobSensor(
    task_id='Wait_for_Replace_Fields_School_2012',
    job_name=job_name,
    run_id=glue_school_2012_replace.output,
    dag=dag
)

job_name="Second Rename Columns INEP Student"
glue_school_2012_rename_second = GlueJobOperator(
    task_id='Second_Rename_School_2012',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Second Rename Columns INEP Student.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/school_out.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/school_replaced.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/PISA2012_SAS_school.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_school_2012_rename_second = GlueJobSensor(
    task_id='Wait_for_Second_Rename_School_2012',
    job_name=job_name,
    run_id=glue_school_2012_rename_second.output,
    dag=dag
)

# ======================================  YEAR 2012, Parent ========================================

job_name="Rename Columns INEP Student"
glue_parent_2012_rename = GlueJobOperator(
    task_id='Rename_Columns_INEP_Parent_2012',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Rename Columns INEP Student.py",
    script_args={"--CSV": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/Data Parent", "--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/parent_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/PISA2012_SAS_parent.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_parent_2012_rename = GlueJobSensor(
    task_id='Wait_for_Rename_Columns_INEP_Parent_2012',
    job_name=job_name,
    run_id=glue_parent_2012_rename.output,
    dag=dag
)

job_name="INEP Student Data ETL Replace Fields"
glue_parent_2012_replace = GlueJobOperator(
    task_id='Replace_Fields_Parent_2012',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/INEP Student Data ETL Replace Fields.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/parent_replaced.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/parent_renamed.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/PISA2012_SAS_parent.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_parent_2012_replace = GlueJobSensor(
    task_id='Wait_for_Replace_Fields_Parent_2012',
    job_name=job_name,
    run_id=glue_parent_2012_replace.output,
    dag=dag
)

job_name="Second Rename Columns INEP Parent"
glue_parent_2012_rename_second = GlueJobOperator(
    task_id='Second_Rename_Parent_2012',
    job_name=job_name,
    wait_for_completion=False,
    script_location="s3://aws-glue-assets-" + AWS_ACCOUNT_INFO + "/scripts/Second Rename Columns INEP Student.py",
    script_args={"--OUT": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/parent_out.parquet", "--PARQUET": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/parent_replaced.parquet", "--SAS": "s3://" + PISA_DATA_BUCKET + "/PISA 2012/PISA2012_SAS_parent.sas"},
    retries=0,
    s3_bucket="aws-glue-assets-" +AWS_ACCOUNT_INFO,
    iam_role_name="S3GlueS3FullAccessRole",
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    dag=dag
)

wait_glue_parent_2012_rename_second = GlueJobSensor(
    task_id='Wait_for_Second_Rename_Parent_2012',
    job_name=job_name,
    run_id=glue_parent_2012_rename_second.output,
    dag=dag
)



# =======================================================================================================
#                                       ORDER TASKS IN DAG
# =======================================================================================================


upload_PISA_2000 >> [lambda_student_2000_math, lambda_student_2000_read, lambda_school_2000, lambda_student_2000_science]
upload_PISA_2003 >> [lambda_student_2003, lambda_school_2003] 
upload_PISA_2006 >> [lambda_student_2006, lambda_school_2006, lambda_parent_2006]
upload_PISA_2009 >> [lambda_student_2009, lambda_school_2009, lambda_parent_2009]
upload_PISA_2012 >> [lambda_student_2012, lambda_school_2012, lambda_parent_2012]

lambda_student_2000_math >> glue_student_2000_math_rename >> wait_glue_student_2000_math_rename >> glue_student_2000_math_replace >> wait_glue_student_2000_math_replace >> glue_student_2000_math_rename_second >> wait_glue_student_2000_math_rename_second
lambda_student_2000_read >> glue_student_2000_read_rename >> wait_glue_student_2000_read_rename >> glue_student_2000_read_replace >> wait_glue_student_2000_read_replace >> glue_student_2000_read_rename_second >> wait_glue_student_2000_read_rename_second
lambda_student_2000_science >> glue_student_2000_science_rename >> wait_glue_student_2000_science_rename >> glue_student_2000_science_replace >> wait_glue_student_2000_science_replace >> glue_student_2000_science_rename_second >> wait_glue_student_2000_science_rename_second
lambda_school_2000 >> glue_school_2000_rename >> wait_glue_school_2000_rename >> glue_school_2000_replace >> wait_glue_school_2000_replace >> glue_school_2000_rename_second >> wait_glue_school_2000_rename_second

lambda_student_2003 >> glue_student_2003_rename >> wait_glue_student_2003_rename >> glue_student_2003_replace >> wait_glue_student_2003_replace >> glue_student_2003_rename_second >> wait_glue_student_2003_rename_second
lambda_school_2003 >> glue_school_2003_rename >> wait_glue_school_2003_rename >> glue_school_2003_replace >> wait_glue_school_2003_replace >> glue_school_2003_rename_second >> wait_glue_school_2003_rename_second

lambda_student_2006 >> glue_student_2006_rename >> wait_glue_student_2006_rename >> glue_student_2006_replace >> wait_glue_student_2006_replace >> glue_student_2006_rename_second >> wait_glue_student_2006_rename_second
lambda_school_2006 >> glue_school_2006_rename >> wait_glue_school_2006_rename >> glue_school_2006_replace >> wait_glue_school_2006_replace >> glue_school_2006_rename_second >> wait_glue_school_2006_rename_second
lambda_parent_2006 >> glue_parent_2006_rename >> wait_glue_parent_2006_rename >> glue_parent_2006_replace >> wait_glue_parent_2006_replace >> glue_parent_2006_rename_second >> wait_glue_parent_2006_rename_second

lambda_student_2009 >> glue_student_2009_rename >> wait_glue_student_2009_rename >> glue_student_2009_replace >> wait_glue_student_2009_replace >> glue_student_2009_rename_second >> wait_glue_student_2009_rename_second
lambda_school_2009 >> glue_school_2009_rename >> wait_glue_school_2009_rename >> glue_school_2009_replace >> wait_glue_school_2009_replace >> glue_school_2009_rename_second >> wait_glue_school_2009_rename_second
lambda_parent_2009 >> glue_parent_2009_rename >> wait_glue_parent_2009_rename >> glue_parent_2009_replace >> wait_glue_parent_2009_replace >> glue_parent_2009_rename_second >> wait_glue_parent_2009_rename_second

lambda_student_2012 >> glue_student_2012_rename >> wait_glue_student_2012_rename >> glue_student_2012_replace >> wait_glue_student_2012_replace >> glue_student_2012_rename_second >> wait_glue_student_2012_rename_second
lambda_school_2012 >> glue_school_2012_rename >> wait_glue_school_2012_rename >> glue_school_2012_replace >> wait_glue_school_2012_replace >> glue_school_2012_rename_second >> wait_glue_school_2012_rename_second
lambda_parent_2012 >> glue_parent_2012_rename >> wait_glue_parent_2012_rename >> glue_parent_2012_replace >> wait_glue_parent_2012_replace >> glue_parent_2012_rename_second >> wait_glue_parent_2012_rename_second