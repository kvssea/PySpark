import configparser
from pyspark.sql import SparkSession

config=configparser.ConfigParser()
config.read('utils/config.ini')

password=config['postgres']['Password']
user=config['postgres']['User']
host=config['postgres']['Host']
jdbc=config['postgres']['JDBC']
port=config['postgres']['Port']

def create_spark_postgres(server_name, table_name):
	spark = SparkSession.builder.config('spark.driver.extraClassPath', jdbc).getOrCreate()
	url = f'jdbc:postgresql://{host}:{port}/{server_name}'
	properties = {'user':user, 'password':password}
	df = spark.read.jdbc(url=url, table=table_name, properties=properties)
	return df


