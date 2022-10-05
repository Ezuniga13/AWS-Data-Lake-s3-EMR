# AWS-Data-Lake-s3-EMR
A data lake that process data using spark.

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
     ![Main Page!](/images/STEP-1.png)
     [Main Page!](/images/STEP-2.png)
     [Main Page!](/images/STEP-3.png)
     [Main Page!](/images/STEP-4.png)
    



- AWS consonle

### How to run the Pyton scripts


**Steps**

1. Bring the all the files in your root directory.
2. In the command terminal move into the root directory.
3. Run the ETL.py in the termain with the command -- python etl.py
4. You can monitor your progess by opening the AWS console i the s3 section.


### Files in the repository


    
-  etl.py
     

- dwh.cfg
  This file is where your aws crendentials are stored. 



### Schema on read

The schema is designed for the analytics team to get insights on what songs their users are listening to and to improve performance by reducing the number of joins.

The fact table labeled as songplays has fields that are comprised from the primary keys for the outer tables.

The outer tables have been denormalized and have user, song, artist and time information that has been duplicated.

