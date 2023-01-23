import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
import boto3
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)


spark = SparkSession.builder.config("spark.sql.shuffle.partitions", 180).config("spark.executor.memory", "10g").config("spark.driver.memory", "10g").getOrCreate()
## spark = SparkSession.builder.getOrCreate()
#spark = glueContext.spark_session

job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME','SAS','PARQUET','OUT'])
job.init(args['JOB_NAME'], args)

#job = Job(glueContext)
#args = getResolvedOptions(sys.argv, ['JOB_NAME'])
#job.init(args['JOB_NAME'], args)
#job.commit()


#def update_df(sas_file_path, csv_file, out_path):

sas_file_path = args["SAS"]
parquet_file = args["PARQUET"]
out_path = args["OUT"]

# csv_file = s3://inep-bucket/PISA 2003/INT_stui_2003_v2_csv.csv
#df = df.iloc[:, :-1]

s3 = boto3.client('s3')
sas_file = s3.get_object(Bucket='inep-bucket', Key=sas_file_path.split("s3://inep-bucket/")[1])["Body"].read().decode("utf-8").splitlines()
    
mappings = {}
#file = open(sas_file, "rt", encoding="utf-8")

original_names = []
human_readable_names = []

start = False
for number, line in enumerate(sas_file):
    if line.replace(" ", "")[0:6].upper() == "FORMAT" and not sas_file[number+1].split(" ")[-1].strip("$").replace("\n", "").strip(" ").strip(".").isnumeric():
        start = True
    if start and ";" in line:
        break
    if start and line.replace(" ", "")[0:6].upper() != "FORMAT":
        non_spaces_in_line = [word for word in line.replace("\t", " ").split(" ") if word != ""]
        try:
            if not non_spaces_in_line[1].replace("\n", "").replace("\"", "").replace(".", "").strip(".").isnumeric():
                mappings[non_spaces_in_line[0].upper()] = non_spaces_in_line[1].replace("\n", "").replace("\"", "").strip(".").upper()
        except:
            if not non_spaces_in_line[1].replace("\n", "").replace("\"", "").replace(".", "").strip(".").isnumeric():
                non_spaces_in_line = [word for word in line.split("	") if word != ""] # UTF-8 U+009 (looks like a space but isn't)
                mappings[non_spaces_in_line[0].upper()] = non_spaces_in_line[1].replace("\n", "").replace("\"", "").strip(".").upper()
                
values_to_replace = {}

start = False
for number, line in enumerate(sas_file):
    if line.replace(" ", "").replace("	", "")[0:5].upper() == "VALUE":
        non_spaces_in_line = [word for word in line.replace("	", " ").split(" ") if word != ""]
        key = non_spaces_in_line[1].replace('\"', "").replace("\n","").replace("\t", "").upper()
        values = {}
        start = True
    if "=" in line and start == True:
        non_spaces_in_line = [word for word in line.split("=")[0].split(" ") if word != ""]
        values[non_spaces_in_line[-1].replace('\"', "").replace("\n","").replace("\t", "").strip(" ").upper()] = line.split("=")[1].replace('\"', "").replace("\n","").replace("\t", "").strip(" ")
    if ";" in line and start == True:
        values_to_replace[key] = values
    if start == True and "run;" in line:
        start == False
        break

df  = spark.read.parquet(parquet_file)

for k in mappings:
    if str("$"+mappings[k]) in values_to_replace or str("$"+mappings[k]).upper() in values_to_replace:
            df = df.replace(values_to_replace["$"+mappings[k].upper()], subset=[k])
    else: 
            df = df.replace(values_to_replace[mappings[k].upper()], subset=[k])
            
# upload to S3

#df.write.option("header","true").parquet(out_path)
df = DynamicFrame.fromDF(df, glueContext, "student_data_out")
glueContext.write_dynamic_frame.from_options(
    frame = df, 
    connection_type = "s3", 
    connection_options =  {"path":out_path},
    format = "parquet")
        
#return

#update_df(args["SAS"], args["CSV"], args["OUT"])