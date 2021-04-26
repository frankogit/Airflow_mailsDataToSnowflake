# Airflow_mailsDataToSnowflake

This app get some filtered mails to a dwh - Snowflake

The steps condidered are:

1.Get from mailbox the filtered mails
2.convert from xlsx to csv
3.Move to S3 - Aws datalake
4.Move to Snowflake datalake

Code is next to this .md file

Following images shows the Dag, this time not found mails to process

![image](https://user-images.githubusercontent.com/5835040/116141758-b9f7cb00-a69e-11eb-8d4d-486bf4158eed.png)
