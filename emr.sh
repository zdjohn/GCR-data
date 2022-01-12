#!/usr/bin/bash

# package 
tox -e pack

# upload to s3 
aws s3 sync dist s3://pyspark3-sample/artifact/

#involke


aws emr create-cluster --name "gcn-corss-data" \
    --release-label emr-6.3.0 \
    --applications Name=Spark \
    --log-uri s3://pyspark3-sample/logs/ \
    --ec2-attributes KeyName=emr-sample-keypair \
    --instance-type c5.xlarge \
    --instance-count 5 \
    --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"}},{"Classification":"spark-env","Configurations":[{"Classification":"export","Properties":{"PYSPARK_PYTHON":"/usr/bin/python3"}}]}]' \
    --bootstrap-action Path="s3://pyspark3-sample/artifact/bootstrap.sh" \
    --steps Type=Spark,Name="gcn-corss-data",ActionOnFailure=TERMINATE_CLUSTER,Args=[--deploy-mode,cluster,--master,yarn,--py-files,s3://theurge-spark/spark-db-work/staging/adhoc/brand_collections/artifact/dist_files.zip,--files,s3://theurge-spark/spark-db-work/staging/adhoc/brand_collections/artifact/settings.json,s3://theurge-spark/spark-db-work/staging/adhoc/brand_collections/artifact/main.py,--raw_source_path,s3://theurge-es-export/products_prod/2021-11-03_20-50-47/,--input_path,s3://theurge-spark/spark-db-work/staging/adhoc/brand_collections/$1/,--output_path,s3://theurge-spark/spark-db-work/staging/adhoc/brand_collections/$1/,--min_tf,$2,--max_df,$3,--top_term,$4] \
    --use-default-roles \
    --auto-terminate