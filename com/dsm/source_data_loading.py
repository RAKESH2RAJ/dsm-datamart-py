from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import os.path
import yaml
import com.dsm.utils.utilities as ut

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()
    # .master('local[*]') \
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    src_list = app_conf["source_list"]
    for src in src_list:
        if src == 'SB':
            txn_df = ut.read_from_mysql(spark, app_secret, app_conf, src) \
                .withColumn("ins_dt", f.current_date())
            txn_df.show()
            txn_df.write \
                .partitionBy("ins_dt") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/staging/" + src)

        if src == 'OL':
            ol_txn_f = ut.read_from_sftp(spark, app_secret, app_conf, src) \
                .withColumn("ins_dt", f.current_date())
            ol_txn_f.show(5, False)
            ol_txn_f.write \
                .partitionBy("ins_dt") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/staging/" + src)

        if src == '1CP':
            cp_df = spark.read \
                .option("inferSchema", "true") \
                .option("header", "true") \
                .option("delimiter", "|") \
                .csv("s3a://" + app_conf[src]["s3_conf"]["s3_bucket"] + "/" + app_conf[src]["s3_conf"]["filename"]) \
                .withColumn("ins_dt", f.current_date())
            cp_df.show()
            cp_df.write \
                .partitionBy("ins_dt") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/staging/" + src)

        spark.stop()

# spark-submit --master yarn --packages "org.apache.hadoop:hadoop-aws:2.7.4,mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1" com/dsm/source_data_loading.py
