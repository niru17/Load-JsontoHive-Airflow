from pyspark.sql import SparkSession
#from pyspark.sql.functions import *

def writee():
    try:
        spark=SparkSession.builder.appName("Write JSON to Hive")\
            .config("spark.sql.warehouse.dir","gs://spark_ex_airflow/assignment2/hive_data/").enableHiveSupport().getOrCreate()
        
        input_path="gs://spark_ex_airflow/assignment2/data/"

        data=spark.read.csv(input_path, header=True, inferSchema=True)
        
        hql1='CREATE DATABASE IF NOT EXISTS EMP_DB'
        spark.sql(hql1)

        hql2='USE EMP_DB'
        spark.sql(hql2)

        hql3='''
            CREATE TABLE IF NOT EXISTS employee(
            emp_id INT,
            emp_name STRING,
            dept_id INT,
            salary INT
            )
            STORED AS PARQUET
            '''
        spark.sql(hql3)

        data.write.mode("append").format("hive").saveAsTable("EMP_DB.employee")
        
    except Exception as e:
        print("Error has occurred: ",e)
    finally:
        spark.stop()

if __name__ == "__main__":
    writee()
