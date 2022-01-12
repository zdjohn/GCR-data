from ..commons import utils
from ..reviews import etl
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession


BASE_PATH = "./data/"


def run(session: SparkSession, logger, settings):
    base_path = settings.get("base_path") or BASE_PATH

    books_df = (
        session.read.parquet(f"{base_path}/books")
        .filter(F.col("star_rating") > 3)
        .select("customer_id", F.col("product_id").alias("book_product_id"))
    )
    music_df = (
        session.read.parquet(f"{base_path}/music")
        .filter(F.col("star_rating") > 3)
        .select("customer_id", F.col("product_id").alias("music_product_id"))
    )

    dense_reviews = etl.dense_cross_domains_reviews(books_df, music_df)
    indexed_and_binned_reviews = etl.binned_indexed_user_reviews(dense_reviews)
    positive_negative_reviews = etl.positive_negative_reviews(
        indexed_and_binned_reviews
    )

    music_graph = etl.get_item_graph(
        indexed_and_binned_reviews, "music_id_indexes", "customer_id_index"
    )
    book_graph = etl.get_item_graph(
        indexed_and_binned_reviews, "book_id_indexs", "customer_id_index"
    )

    indexed_and_binned_reviews.coalesce(1).write.parquet(
        f"{base_path}/indexed_and_binned_reviews", mode="overwrite"
    )

    positive_negative_reviews.coalesce(1).write.parquet(
        f"{base_path}/binned_reviews", mode="overwrite"
    )
    book_graph.coalesce(1).write.parquet(f"{base_path}/books_graph", mode="overwrite")
    music_graph.coalesce(1).write.parquet(f"{base_path}/music_graph", mode="overwrite")
