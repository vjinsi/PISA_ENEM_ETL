# PISA ENEM ETL

This repository contains code necessary to transform data about the ENEM and PISA exams, and save them into more suitable files for further analysis.

In order to execute this code, an AWS account is necessary. 

The _Unzip_and_to_CSV.py_ file should be used to define a Lambda function, whereas the files _INEP Student Data ETL Replace Fields.py_, _Rename COlumns INEP Student.py_ and _Second Rename Columns INEP Student.py_ should define Glue scripts.

The files within the _dags_ folder define Airflow DAGs. In order to run the DAGs, simply define the previously mentioned Glue and Lambda scripts, and update the variables PISA_DATA_BUCKET and AWS_ACCOUNT_INFO.