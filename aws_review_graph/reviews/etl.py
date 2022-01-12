from pyspark import SparkConf
from pyspark import SparkFiles
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import ArrayType, StringType, IntegerType
import pandas as pd
import random

# +-----+-----------+-----------+-------------+-------------+
# |users|total_books|total_music|distinc_books|distinc_music|
# +-----+-----------+-----------+-------------+-------------+
# | 5668|     136074|      88207|       112278|        59092|
# +-----+-----------+-----------+-------------+-------------+

# +-------+------------------+-----------------+
# |summary|       books_count|      music_count|
# +-------+------------------+-----------------+
# |  count|              5668|             5668|
# |   mean|24.007410021171488|15.56227946365561|
# | stddev|11.046380945293786|6.239598938153374|
# |    min|                12|                9|
# |    25%|                15|               11|
# |    50%|                20|               14|
# |    75%|                30|               19|
# |    max|                55|               33|
# +-------+------------------+-----------------+

NUMBER_OF_BIN = 10
SEED = 1
BOOK_DISTINCT = 112278
MUSIC_DISTINCT = 59092

BOOK_MIN = 12
MUSIC_MIN = 9

BOOK_MAX = 55
MUSIC_MAX = 33


def dense_cross_domains_reviews(
    source_domain: DataFrame,
    target_domain: DataFrame,
    book_min: int = BOOK_MIN,
    music_min: int = MUSIC_MIN,
    book_max: int = BOOK_MAX,
    music_max: int = MUSIC_MAX,
) -> DataFrame:
    """[summary]

    Args:
        source_domain (DataFrame): [description]
        target_domain (DataFrame): [description]
        book_min (int, optional): [description]. Defaults to BOOK_MIN.
        music_min (int, optional): [description]. Defaults to MUSIC_MIN.
        book_max (int, optional): [description]. Defaults to BOOK_MAX.
        music_max (int, optional): [description]. Defaults to MUSIC_MAX.

    Returns:
        DataFrame: [description]
        root
        |-- customer_id: string (nullable = true)
        |-- book_ids: array (nullable = true)
        |    |-- element: string (containsNull = true)
        |-- music_ids: array (nullable = true)
        |    |-- element: string (containsNull = true)
        |-- books_count: integer (nullable = true)
        |-- music_count: integer (nullable = true)
    """
    joined_df = source_domain.join(target_domain, on=["customer_id"])
    joined_customer_df = (
        joined_df.groupBy("customer_id")
        .agg(
            F.collect_set("book_product_id").alias("book_ids"),
            F.collect_set("music_product_id").alias("music_ids"),
        )
        .withColumn("books_count", F.size("book_ids"))
        .withColumn("music_count", F.size("music_ids"))
        .filter(
            f"music_count>={music_min} and books_count>={book_min} and books_count<={book_max} and music_count<={music_max}"
        )
    )
    return joined_customer_df


def binned_indexed_user_reviews(source_target_user_reviews: DataFrame) -> DataFrame:

    user_indexed_df = to_indexed_ids(source_target_user_reviews, "customer_id")

    book_indexed_df = to_indexed_ids(
        source_target_user_reviews.select(
            F.explode("book_ids").alias("book_id")
        ).dropDuplicates(),
        "book_id",
    )
    music_indexed_df = to_indexed_ids(
        source_target_user_reviews.select(
            F.explode("music_ids").alias("music_id")
        ).dropDuplicates(),
        "music_id",
    )

    user_book_df = (
        user_indexed_df.withColumn("book_id", F.explode("book_ids"))
        .join(book_indexed_df, on=["book_id"])
        .groupby(
            "customer_id",
            "customer_id_index",
            "books_count",
            "music_count",
        )
        .agg(
            F.collect_set("book_id").alias("book_ids"),
            F.collect_set("book_id_index").alias("book_id_indexes"),
        )
    )

    user_music_df = (
        source_target_user_reviews.select(
            "customer_id", F.explode("music_ids").alias("music_id")
        )
        .join(music_indexed_df, on=["music_id"])
        .groupby("customer_id")
        .agg(
            F.collect_set("music_id").alias("music_ids"),
            F.collect_set("music_id_index").alias("music_id_indexes"),
        )
    )

    user_music_book_with_index = user_book_df.join(
        user_music_df, on=["customer_id"]
    ).withColumn("bin", (F.rand(seed=SEED) * NUMBER_OF_BIN).cast("int"))

    user_music_book_with_index.printSchema()
    return user_music_book_with_index


def positive_negative_reviews(
    session: SparkSession,
    binned_indexed_reviews: DataFrame,
    book_distinct: int = BOOK_DISTINCT,
    music_distinct: int = MUSIC_DISTINCT,
) -> DataFrame:
    """[summary]
    +-----------+-----------+
    |users_count|books_count|
    +-----------+-----------+
    |       1369|      39975|
    +-----------+-----------+
    +-----------+------------+
    |users_count|musics_count|
    +-----------+------------+
    |       1369|       23871|
    +-----------+------------+

    Args:
        session (SparkSession): [description]
        binned_indexed_reviews (DataFrame): [description]

    Returns:
        DataFrame:
        root
        |-- customer_id: string (nullable = true)
        |-- customer_id_index: integer (nullable = true)
        |-- book_id_indexes: array (nullable = true)
        |    |-- element: integer (containsNull = true)
        |-- negative_books: array (nullable = true)
        |    |-- element: long (containsNull = true)
        |-- music_id_indexes: array (nullable = true)
        |    |-- element: integer (containsNull = true)
        |-- negative_music: array (nullable = true)
        |    |-- element: long (containsNull = true)
        |-- bin: integer (nullable = true)
    """

    book_indices = [{"all_books": list(range(1, book_distinct + 1))}]
    all_book_df = session.createDataFrame(book_indices)
    music_indices = [{"all_music": list(range(1, music_distinct + 1))}]
    all_music_df = session.createDataFrame(music_indices)

    positive_negative_reviews = (
        binned_indexed_reviews.crossJoin(all_book_df)
        .withColumn(
            "negative_books_except", F.array_except("all_books", "book_id_indexes")
        )
        .withColumn(
            "negative_books",
            F.expr("slice(shuffle(negative_books_except), 1, size(book_id_indexes))"),
        )
        .crossJoin(F.broadcast(all_music_df))
        .withColumn(
            "negative_music_except", F.array_except("all_music", "music_id_indexes")
        )
        .withColumn(
            "negative_music",
            F.expr("slice(shuffle(negative_music_except), 1, size(music_id_indexes))"),
        )
        .select(
            "customer_id",
            "customer_id_index",
            "book_id_indexes",
            "negative_books",
            "music_id_indexes",
            "negative_music",
            "bin",
        )
    )

    positive_negative_reviews.printSchema()

    return positive_negative_reviews


def user_positive_negative_explode(
    positive_negative_reviews,
    user_col: str = "customer_id_index",
    positive_col: str = "book_id_indexes",
    negative_col: str = "negative_books",
    split_seed: int = SEED,
    split_number: int = 10,
):
    """[summary]

    Args:
        positive_negative_reviews ([type]): [description]
        user_col (str, optional): [description]. Defaults to "customer_id_index".
        positive_col (str, optional): [description]. Defaults to "book_id_indexes".
        negative_col (str, optional): [description]. Defaults to "negative_books".

    Returns:
        [DataFrame]: [description]
        root
        |-- user: integer (nullable = true)
        |-- bin: integer (nullable = true)
        |-- positive: integer (nullable = true)
        |-- negative: long (nullable = true)
    """
    pn_explode = (
        positive_negative_reviews.withColumn("positive", F.explode(positive_col))
        .withColumn("split", (F.rand(seed=split_seed) * split_number).cast("int"))
        .withColumn("negative", F.explode(negative_col))
        .select(F.col(user_col).alias("user"), "bin", "positive", "negative", "split")
        .dropDuplicates()
    )
    return pn_explode


def get_item_graph(matrix_df: DataFrame, vertex_col: str) -> DataFrame:
    graph_df = matrix_df.select(F.col(vertex_col).alias("n")).withColumn(
        "node", F.explode("n")
    )
    graph_df = graph_df.groupBy("node").agg(
        F.array_distinct(F.flatten(F.collect_set("n"))).alias("neighbors"),
        F.count("node").alias("counts"),
    )
    graph_df = graph_df.withColumn(
        "neighbors", F.array_except("neighbors", F.array(F.col("node")))
    ).withColumn("neighbors_size", F.size("neighbors"))
    return graph_df


def review_agg(review_df: DataFrame):
    """[summary]

    Args:
        review_df (DataFrame):
        root
    |-- customer_id: string (nullable = true)
    |-- book_ids: array (nullable = true)
    |    |-- element: string (containsNull = false)
    |-- music_ids: array (nullable = true)
    |    |-- element: string (containsNull = false)
    |-- books_count: integer (nullable = false)
    |-- music_count: integer (nullable = false)

    Returns:
        [type]: [description]
    """
    agg_df = review_df.agg(
        F.count("customer_id").alias("users"),
        F.sum("books_count").alias("total_books"),
        F.sum("music_count").alias("total_music"),
        F.size(F.array_distinct(F.flatten(F.collect_set("book_ids")))).alias(
            "distinc_books"
        ),
        F.size(F.array_distinct(F.flatten(F.collect_set("music_ids")))).alias(
            "distinc_music"
        ),
    )
    return agg_df


def to_indexed_ids(df: DataFrame, col_name: str) -> DataFrame:
    """node id to index number.

    Args:
        df (DataFrame): source data frame.
        col_name (str): column to generate index.

    Returns:
        DataFrame:
        ```
        +-----------------+-----------+..+
        |customer_id_index|customer_id|..|
        +-----------------+-----------+..|
        |                1|   10001105|..|
        |                2|   10007421|..|
        |                3|   10008274|..|
        |                4|   10010722|..|
        |                5|   10012171|..|
        +-----------------+-----------+--+
        ```
    """
    # ref: https://towardsdatascience.com/adding-sequential-ids-to-a-spark-dataframe-fa0df5566ff6
    # index 0 will be reserved for embedding padding
    index_window = Window.orderBy(F.col(col_name))
    index_df = df.withColumn(f"{col_name}_index", F.row_number().over(index_window))
    return index_df


def dense_books_stats(dense_reviews):
    dense_book = dense_reviews.select(
        "customer_id", F.explode("book_ids").alias("book_id")
    ).agg(
        F.countDistinct("customer_id").alias("users_count"),
        F.countDistinct("book_id").alias("books_count"),
    )
    dense_book.show()


def dense_music_stats(dense_reviews):
    dense_music = dense_reviews.select(
        "customer_id", F.explode("music_ids").alias("music_id")
    ).agg(
        F.countDistinct("customer_id").alias("users_count"),
        F.countDistinct("music_id").alias("musics_count"),
    )
    dense_music.show()


@pandas_udf(ArrayType(IntegerType()))
def negative_books(list_col_df: pd.Series) -> pd.Series:
    res = []
    for row in list_col_df:
        negative = list(random.shuffle(set(range(BOOK_DISTINCT)) - set(row)))
        res.append(negative[: len(row)])
    return pd.Series(res)


@pandas_udf(ArrayType(IntegerType()))
def negative_music(list_col_df: pd.Series) -> pd.Series:
    res = []
    for row in list_col_df:
        negative = list(random.shuffle(set(range(MUSIC_DISTINCT)) - set(row)))
        res.append(negative[: len(row)])
    return pd.Series(res)
