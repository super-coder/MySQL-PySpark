from pyspark.sql import SparkSession
import yaml

config_file = open('config.yaml')
configs = yaml.load(config_file, yaml.FullLoader)
user = configs['user']
password = configs['password']

appName = 'PySpark mysql'
master = 'local'

spark = SparkSession.builder.config('spark.jars', '/opt/spark/jars/mysql-connector-java-8.0.28.jar')\
                            .config("spark.driver.bindAddress", "127.0.0.1") \
                            .master(master).appName(appName).getOrCreate()

query = """(WITH tbl1
     AS (SELECT *,
                Round(Lag(adj_close, 1)
                        over (
                          ORDER BY DATE ASC), 2) AS Prev_close
         FROM   TECL)
SELECT Row_number()
         over(
           ORDER BY DATE DESC )         AS id,
       DATE,
       open,
       high,
       low,
       CLOSE,
       adj_close,
       prev_close,
       volume,
       Round(adj_close - prev_close, 2) AS DIFF,
       CASE
         WHEN adj_close - prev_close < 0 THEN 'Down'
         WHEN adj_close - prev_close > 0 THEN 'Up'
         ELSE 'No Change'
       END                              AS STATUS
FROM   tbl1 ) q1"""

df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/financial_data") \
                              .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", query)\
                              .option("user", user).option("password", password).load()

df.show()