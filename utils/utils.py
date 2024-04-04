import configparser
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

config=configparser.ConfigParser()
config.read('utils/config.ini')

password=config['postgres']['Password']
user=config['postgres']['User']
host=config['postgres']['Host']
jdbc=config['postgres']['JDBC']
port=config['postgres']['Port']

def create_spark_postgres(server_name:str, table_name:str) -> DataFrame:
	"""Creates a single-node local pyspark session to connect to local hosted PostgreSQL database"""
	spark = SparkSession.builder.config('spark.driver.extraClassPath', jdbc).getOrCreate()
	url = f'jdbc:postgresql://{host}:{port}/{server_name}'
	properties = {'user':user, 'password':password}
	df = spark.read.jdbc(url=url, table=table_name, properties=properties)
	return df

def create_spark_session(app_name:str=None) -> SparkSession:
	"""Create a simple single-node local Spark Session"""

	if app_name:
		spark = (SparkSession
			.builder
			.getOrCreate())
		return spark
	else:
		spark = (SparkSession
			.builder
			.appName(app_name)
			.getOrCreate())
		return spark



