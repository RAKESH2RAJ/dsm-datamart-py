import com.dsm.utils.utilities as ut
import os.path

def read_from_mysql(spark, app_secret, app_conf, src):
    jdbcParams = {"url": ut.get_mysql_jdbc_url(app_secret),
                  "lowerBound": "1",
                  "upperBound": "100",
                  "dbtable": app_conf[src]["mysql_conf"]["dbtable"],
                  "numPartitions": "2",
                  "partitionColumn": app_conf[src]["mysql_conf"]["partition_column"],
                  "user": app_secret["mysql_conf"]["username"],
                  "password": app_secret["mysql_conf"]["password"]
                  }

    print("\nReading data from MySQL DB using SparkSession.read.format(),")
    return spark \
        .read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .options(**jdbcParams) \
        .load()

def read_from_sftp(spark, app_secret, app_conf, src):
    current_dir = os.path.abspath(os.path.dirname(__file__))
    return spark.read \
        .format("com.springml.spark.sftp") \
        .option("host", app_secret["sftp_conf"]["hostname"]) \
        .option("port", app_secret["sftp_conf"]["port"]) \
        .option("username", app_secret["sftp_conf"]["username"]) \
        .option("pem", os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"])) \
        .option("fileType", "csv") \
        .option("delimiter", "|") \
        .load(app_conf["sftp_conf"]["directory"] + "/" + app_conf[src]["sftp_conf"]["filename"])


def get_redshift_jdbc_url(redshift_config: dict):
    host = redshift_config["redshift_conf"]["host"]
    port = redshift_config["redshift_conf"]["port"]
    database = redshift_config["redshift_conf"]["database"]
    username = redshift_config["redshift_conf"]["username"]
    password = redshift_config["redshift_conf"]["password"]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)


def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["mysql_conf"]["hostname"]
    port = mysql_config["mysql_conf"]["port"]
    database = mysql_config["mysql_conf"]["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)
