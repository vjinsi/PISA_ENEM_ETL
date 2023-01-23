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
args = getResolvedOptions(sys.argv, ['JOB_NAME','SAS','CSV','OUT'])
job.init(args['JOB_NAME'], args)

#job = Job(glueContext)
#args = getResolvedOptions(sys.argv, ['JOB_NAME'])
#job.init(args['JOB_NAME'], args)
#job.commit()


#def update_df(sas_file_path, csv_file, out_path):

sas_file_path = args["SAS"]
csv_file = args["CSV"]
out_path = args["OUT"]

# csv_file = s3://inep-bucket/PISA 2003/INT_stui_2003_v2_csv.csv
#df = df.iloc[:, :-1]

s3 = boto3.client('s3')
sas_file = s3.get_object(Bucket='inep-bucket', Key=sas_file_path.split("s3://inep-bucket/")[1])["Body"].read().decode("utf-8").splitlines()

#bucket = s3.Bucket('inep-bucket')

# sas_file = 'PISA 2003/PISA2003_SAS_student.txt'
#sas_file = bucket.Object(key=sas_file_path.split("s3://inep-bucket/")[0]).get()['Body']

original_names = []
human_readable_names = {}

start = False
for number, line in enumerate(sas_file):
    if "input" in line:
        start = True
    if start and "-" in line:
        non_spaces_in_line = [word for word in line.replace("\t", " ").split(" ") if word != ""]
        original_names.append(non_spaces_in_line[0].upper())
    if start and ";" in line:
        break
        
start = False
for number, line in enumerate(sas_file):
    if line.replace(" ", "")[0:5] == "label" or line.replace(" ", "")[0:5] == "LABEL":
        start = True
    if start and "=" in line:
        human_readable_names[line.split("=")[0].replace("\t", "").strip(" ").upper()] = line.split("=")[1].replace("\t", "").replace("\n", "").replace("\"", "").strip(" ")
    if start and ";" in line:
        break

#df = glueContext.create_dynamic_frame_from_options(connection_type = "s3", connection_options = {"paths": [csv_file]}, format = "csv")
df  = spark.read.format('csv').option('mode', 'DROPMALFORMED').option('inferSchema', 'true').load(csv_file)
#df = pd.read_csv(csv_file, header = None, dtype=str)

###df = df.toDF()

df = df.drop(df.columns[-1])

for index, column in enumerate(df.columns):
    df = df.withColumnRenamed(df.columns[index], original_names[index].upper())
    # df.rename(columns={df.columns[index]: original_names[index].upper()}, inplace=True)

#df.write.option("header","true").parquet(out_path)
df = DynamicFrame.fromDF(df, glueContext, "student_data_out")
glueContext.write_dynamic_frame.from_options(
    frame = df, 
    connection_type = "s3", 
    connection_options =  {"path":out_path},
    format = "parquet")
        
#return

#update_df(args["SAS"], args["CSV"], args["OUT"])