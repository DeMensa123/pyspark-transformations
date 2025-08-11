from pyspark.sql.functions import col, when, to_date, date_format, regexp_extract, udf
from pyspark.sql.types import StringType


def add_claim_type(df):
    return df.withColumn(
        "claim_type",
        when(col("claim_id").startswith("CL"), "Coinsurance")
        .when(col("claim_id").startswith("RX"), "Reinsurance")
        .otherwise(None)
    )


def add_claim_priority(df):
    return df.withColumn(
        "claim_priority", when(col("claim_amount") > 4000, "Urgent")
        .otherwise("Normal")
    )


def add_claim_period(df):
    return df.withColumn(
        "claim_date",
        to_date(col("claim_date"), "yyyy-MM-dd").cast(StringType())
    ).withColumn(
        "claim_period",
        date_format(col("claim_date"), "yyyy-MM").cast(StringType())
    )


def add_source_system_id(df):
    return df.withColumn(
        "source_system_id",
        regexp_extract(col("claim_id"), r"_(\d+)", 1)
    )


def add_hash_id(df, hash_map):
    """
    hash_map: dict mapping claim_id -> hash_id
    """
    def lookup_hash(claim_id):
        return hash_map.get(claim_id)
    lookup_hash_udf = udf(lookup_hash, StringType())
    return df.withColumn("hash_id", lookup_hash_udf(col("claim_id")))


def remove_duplicates(df):
    return df.dropDuplicates(["claim_id"])
