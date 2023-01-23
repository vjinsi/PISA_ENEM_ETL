link={{params.link}}
year={{params.year}}

wget -O- $link | aws s3 cp - "s3://inep-bucket/PISA $year/"