import configparser
import os
from os import environ
import json
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame, Window, functions as F
from .spark_log4j import Log4j


# ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
# CONFIG_PATH = os.path.join(ROOT_DIR, 'spark.conf')

AWS_CATEGORY_RAW = (
    "s3a://amazon-reviews-pds/parquet/product_category={category}/*.snappy.parquet"
)

SOURCE_REVIEWS_PATH = "{source_domain}{target_domain}/{source_domain}_reviews"
TARGET_REVIEWS_PATH = "{source_domain}{target_domain}/{target_domain}_reviews"
CUSTOMERS_INDEXED_IDS_PATH = "{source_domain}{target_domain}/common_customer_ids"
SOURCE_PRODUCT_INDEXED_IDS_PATH = (
    "{source_domain}{target_domain}/{source_domain}_product_ids"
)
TARGET_PRODUCT_INDEXED_IDS_PATH = (
    "{source_domain}{target_domain}/{target_domain}_product_ids"
)

PRODUCT_EDGES_PATH = "{source_domain}{target_domain}/product_edges/"
CUSTOMER_EDGES_PATH = "{source_domain}{target_domain}/customer_edges/"

PN_SAMPLES_PATH = "{source_domain}{target_domain}/positive_negative_samples/"


def load_settings(kwargs):
    settings = {}
    with open("./settings.json") as f:
        settings = json.load(f)

    for key in kwargs:
        if kwargs.get(key):
            settings[key] = kwargs[key]

    if not settings.get("s3a_path"):
        raise Exception("no target s3a path to save data")

    settings["raw_path"] = AWS_CATEGORY_RAW.format(
        category=settings.get("category", "")
    )

    # settings['target_raw_path'] = SOURCE_REVIEWS_PATH.format(
    #     target_domain=settings['target_domain'])

    settings["source_reviews_path"] = SOURCE_REVIEWS_PATH.format(
        source_domain=settings.get("source_domain", ""),
        target_domain=settings.get("target_domain", ""),
    )
    settings["target_reviews_path"] = TARGET_REVIEWS_PATH.format(
        source_domain=settings.get("source_domain", ""),
        target_domain=settings.get("target_domain", ""),
    )
    settings["customers_indexed_ids_path"] = CUSTOMERS_INDEXED_IDS_PATH.format(
        source_domain=settings.get("source_domain", ""),
        target_domain=settings.get("target_domain", ""),
    )
    settings[
        "source_product_indexed_ids_path"
    ] = SOURCE_PRODUCT_INDEXED_IDS_PATH.format(
        domain=settings.get("source_domain", ""),
        source_domain=settings.get("source_domain", ""),
        target_domain=settings.get("target_domain", ""),
    )
    settings[
        "target_product_indexed_ids_path"
    ] = TARGET_PRODUCT_INDEXED_IDS_PATH.format(
        domain=settings.get("target_domain", ""),
        source_domain=settings.get("source_domain", ""),
        target_domain=settings.get("target_domain", ""),
    )
    settings["customers_edges_by_target_domain"] = CUSTOMER_EDGES_PATH.format(
        domain=settings.get("target_domain", ""),
        source_domain=settings.get("source_domain", ""),
        target_domain=settings.get("target_domain", ""),
    )
    settings["customers_edges_by_source_domain"] = CUSTOMER_EDGES_PATH.format(
        domain=settings.get("source_domain", ""),
        source_domain=settings.get("source_domain", ""),
        target_domain=settings.get("target_domain", ""),
    )
    settings["customer_edges"] = CUSTOMER_EDGES_PATH.format(
        source_domain=settings.get("source_domain", ""),
        target_domain=settings.get("target_domain", ""),
    )
    settings["product_edges"] = PRODUCT_EDGES_PATH.format(
        source_domain=settings.get("source_domain", ""),
        target_domain=settings.get("target_domain", ""),
    )
    settings["pn_samples"] = PN_SAMPLES_PATH.format(
        source_domain=settings.get("source_domain", ""),
        target_domain=settings.get("target_domain", ""),
    )

    return settings


def s3_credential(session: SparkSession):
    config = configparser.ConfigParser()
    config.read(os.path.expanduser("~/.aws/credentials"))
    access_id = config.get("default", "aws_access_key_id")
    access_key = config.get("default", "aws_secret_access_key")
    hadoop_conf = session._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", access_id)
    hadoop_conf.set("fs.s3a.secret.key", access_key)


def get_spark_app_config(configs: dict):
    spark_conf = SparkConf()

    for key, val in configs.items():
        spark_conf.set(key, val)
    return spark_conf


def start_spark(**kwargs):
    """[summary]
    jar_packages=[], files=[],

    Args:
        jar_packages (list, optional): [description]. Defaults to [].
        files (list, optional): [description]. Defaults to [].

    Returns:
        [type]: [description]
    """
    # detect execution environment
    flag_debug = "DEBUG" in environ.keys()

    settings = load_settings(kwargs)

    spark_builder = SparkSession.builder

    print("getting spark session")

    spark_conf = get_spark_app_config(settings["spark_app_configs"])
    spark_builder.config(conf=spark_conf)

    # create session and retrieve Spark logger object
    spark_session = spark_builder.getOrCreate()
    spark_session.sparkContext.setLogLevel(settings.get("logLevel", "INFO"))
    spark_logger = Log4j(spark_session)

    if flag_debug:
        s3_credential(spark_session)

    print("spark session created")

    return spark_session, spark_logger, settings


def action_describe(dataframe: DataFrame, columns: list) -> None:
    """table statistics, mean, min, max, count, stddev

    Args:
        df (DataFrame): target dataframe
        columns (list): columns to describe
    """
    df_describe = dataframe.describe(columns)
    df_describe.show()
