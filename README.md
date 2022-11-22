# AWS-Data-Lake-s3-EMR-Spark
A data lake that processes data using spark.

Esteban Zuniga <br>
October 5, 2022 <br>
Data Engineer

## Overview

A consulting analytics team has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I built an ETL pipeline that extracts their data from S3, processes it using Spark, and loads the data back into the S3 lake as a set of dimensional tables. I've deployed this Spark process on a cluster using AWS EMR, IAM, S3, CLI, EC2, and jupyterlab.

### How to generate credentials.
1. Go to your AWS account and go to the IAM console.
2. Create a user with administration acess.
3. This will produce an acces key id and password, put these credentials in the dl.cfg file.
4. Go to the EC2 and create a key pair with whatever name you want.
5. Go to the EC2 security group and get the subnet ID and save it somewhere. 

### How to spin up an EMR cluster.
- CLI
    1. Install cli https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
    2. Go to the root directory and run the following commands it will prompt for credentials, region and format.
     ![Main Page!](/images/STEP-1.png) <br>
     ![Main Page!](/images/STEP-2.png) <br>
     ![Main Page!](/images/STEP-3.png) <br>
     ![Main Page!](/images/STEP-4a.png)
    



- AWS consonle

### How to run the Pyton scripts


**Steps**

1. Bring the all the files in your root directory.
2. In the command terminal move into the root directory.
3. Run the ETL.py in the termain with the command -- python etl.py
4. You can monitor your progess by opening the AWS console i the s3 section.


### Files in the repository


    
-  etl.py
    This file has the functions that processes the song and log data and throws it into a data lake.
- dwh.cfg
  This file is where your aws crendentials are stored. 



### Schema on read

Spark is a schema on read tech. Take note that there is no sql python scripts because with spark it can manipulate raw data as it is read in. The tables were designed before writing and partitioning them to the s3 lake. I paritioned the data in by month and year as a wrote them to a parquet file to as a technique to process BIG DATA. This particular data set has massive of data points and doing this makes it possible to process the data faster.

