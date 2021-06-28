from pyspark import SparkSession

Spark = SparkSession.builder.master("local").getOrCreate()
endpoint = "https://minioprod/location"
user = "service_account"
password = "xyzxys"
bucketName = "usage"

spark._jsc.hadoopConfiguration().set("fs.s3a.bucketname.connection.ssl.enabled", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.bucketname.endpoint", endpoint)
spark._jsc.hadoopConfiguration().set("fs.s3a.bucketname.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.bucketname.access.key", user)
spark._jsc.hadoopConfiguration().set("fs.bucketname.secret.key", password)

source_df = spark.read.option("delimiter", ",").option(
    "header", "true").csv("s3a://dx.dl.bucketname.stats_usage")
source_df.printSchema()
