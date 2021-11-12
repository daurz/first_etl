from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.models import Variable
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowSensorTimeout
from datetime import datetime
import requests as r
from xml.etree import ElementTree
import urllib
import json
import csv

default_args = {'start_date': datetime(2021, 11, 10)}


def S3KeySensor():
   
    resp = r.get("https://storage.yandexcloud.net/misis-zo-bigdata?list-type=2&encoding-type=url")
    tree = ElementTree.fromstring(resp.text)    
    if len(tree) >= 7:
        return True
    else:
        return False


def S3Json2CSV():

    def getRows(data, columns):
        output = []
        output.append(columns)
        for i, line in enumerate(data):
            row = json.loads(line.decode('utf-8'))
            output.append([row[key] if key in row.keys() else '' for key in columns])
        return output

    key = ElementTree.fromstring(r.get("https://storage.yandexcloud.net/misis-zo-bigdata?list-type=2&encoding-type=url").text)[6][0].text

    base_url = "https://storage.yandexcloud.net/misis-zo-bigdata/"
    data = urllib.request.urlopen(base_url+key)
    columns = ['overall', 'verified', 'reviewTime', 'reviewerID', 'asin', 'reviewerName', 'reviewText', 'summary', 'unixReviewTime']
    fname = "/tmp/new_data_dauren.csv"
    with open(fname, 'w') as outf:
        outcsv = csv.writer(outf)
        outcsv.writerows(getRows(data, columns))


def _partner_b():
    return True


def _failure_callback(context):
    if isinstance(context['exception'], AirflowSensorTimeout):
        print(context)
        print("Sensor timed out")

with DAG('dauren_dag', schedule_interval='*/30 * * * *', default_args=default_args, catchup=False) as dag:
    s3_check = PythonSensor(
        task_id='s3keysensor',
        poke_interval=120,
        timeout=30,
        mode="reschedule",
        python_callable=S3KeySensor,
        on_failure_callback=_failure_callback,
        soft_fail=True
    )

    json2csv = PythonOperator(
        task_id="json2csv",
        python_callable=S3Json2CSV,
        trigger_rule='none_failed_or_skipped'
    )

    copy_hdfs_task = BashOperator(
        task_id='copy_hdfs_task', 
        bash_command='hdfs dfs -put -f /tmp/new_data_dauren.csv /user/dauren_naipov/staging/new_data.csv'
    )

    hql = """DROP TABLE IF EXISTS data_tmp;
            CREATE EXTERNAL TABLE data_tmp (
	                            overall numeric(2,1),
	                            verified boolean,
	                            reviewtime string,
                              reviewerid string,
                              asin string,
                              reviewername string,
                              reviewtext string,
                              summary string,
                              unixreviewtime int)
            ROW FORMAT delimited fields terminated by ','
            STORED AS TEXTFILE
            LOCATION '/user/dauren_naipov/staging/';"""

    hql1 = """INSERT INTO TABLE all_raitings
            SELECT overall, verified, from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy-MM-dd') as reviewtime,
            reviewerid, asin, reviewername, reviewtext, summary, unixreviewtime,
            from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM data_tmp;"""


    hql2 = """INSERT INTO TABLE user_scores SELECT reviewerid, asin, overall, reviewtime,
            from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM data_tmp;"""

    hql3 = """INSERT INTO TABLE reviews SELECT reviewerid, reviewtext, overall, reviewtime,
            from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM data_tmp;"""

    hql4 = """INSERT INTO TABLE product_scores SELECT asin, overall, reviewtime,
            from_unixtime(unix_timestamp(reviewtime,'MM dd, yyyy'),'yyyy') as part_year FROM data_tmp;"""
   

    hive_load = HiveOperator(
        hql=hql,
        hive_cli_conn_id='hive_staging',
        schema='dauren_naipov',
        hiveconf_jinja_translate=True,
        task_id='hive_load',
        dag=dag)

    parquet_all_raitings = HiveOperator(
        hql=hql1,
        hive_cli_conn_id='hive_staging',
        schema='dauren_naipov',
        hiveconf_jinja_translate=True,
        task_id='parquet_all_raitings',
        dag=dag)

    parquet_scores = HiveOperator(
        hql=hql2,
        hive_cli_conn_id='hive_staging',
        schema='dauren_naipov',
        hiveconf_jinja_translate=True,
        task_id='parquet_scores',
        dag=dag)

    parquet_reviews = HiveOperator(
        hql=hql3,
        hive_cli_conn_id='hive_staging',
        schema='dauren_naipov',
        hiveconf_jinja_translate=True,
        task_id='parquet_reviews',
        dag=dag)

    parquet_product_scores = HiveOperator(
        hql=hql4,
        hive_cli_conn_id='hive_staging',
        schema='dauren_naipov',
        hiveconf_jinja_translate=True,
        task_id='parquet_product_scores',
        dag=dag)

s3_check >> json2csv >> copy_hdfs_task >> hive_load >> parquet_all_raitings >> parquet_scores >> parquet_reviews >> parquet_product_scores
