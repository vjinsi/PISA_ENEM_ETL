links={{params.links}}
year={{params.year}}

for link in $links;
do
    if [[ "$(echo $link | rev | cut -d'.' -f1)" == *"sas"* ]] ;
        then name=$(echo $link | rev | cut -d"/" -f1 | rev);
        else name=$(echo $link | rev | cut -d"/" -f1 | rev | cut -d"." -f1);
        fi
    wget -O- $link | aws s3 cp - "s3://inep-bucket/PISA $year/"
    aws s3 mv "s3://inep-bucket/PISA $year/-" "s3://inep-bucket/PISA $year/$name"
done