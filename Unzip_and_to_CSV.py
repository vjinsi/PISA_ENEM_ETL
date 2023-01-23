import json
import zipfile
import boto3
from io import BytesIO
import re

def convert_to_csv(sas_file, in_file, out_path, limits):
    
    s3_resource = boto3.resource('s3')
    
    numbers = []
    
    fields = s3_resource.Object(bucket_name = "inep-bucket", key = sas_file).get()["Body"].read()
    fields = str(fields)
    
    for number, line in enumerate(fields.split("\\r\\n")):
        if number >= limits[0]-1 and number < limits[1]:
            line = line.replace("\\t","\t")
            numbers.append([str(re.findall("(?:[ \t$])([0-9]+){1,}",line.split("-")[0])[0]),str(re.findall("([0-9]+){1,}",line.split("-")[-1])[0])])
    
    fout = ""
    
    for line in in_file:
        for number in numbers:
            str_line = line.decode()
            fout += str_line[int(number[0])-1:int(number[1])]+','
        fout += "\n"
    
    s3_resource.Object('inep-bucket', out_path).put(Body=fout)
    
    return

def lambda_handler(event, context):

    bucket = "inep-bucket"
    sas_file = event["sas_file"]
    in_path = event["in_path"]
    out_path = event["out_path"]
    limits = tuple(map(int, event["limits"].split(",")))
    
    s3_resource = boto3.resource('s3')
    zip_obj = s3_resource.Object(bucket_name=bucket, key=in_path)

    buffer = BytesIO(zip_obj.get()["Body"].read())
    z = zipfile.ZipFile(buffer)
    
    for filename in z.namelist():
        with z.open(filename) as in_file:
            convert_to_csv(sas_file, in_file, out_path, limits)
    
    return
