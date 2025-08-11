import requests
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

from config import logger


def get_md4_hash(claim_id: str) -> str | None:
    """
    Call external API to get MD4 hash for claim_id.
    """

    url = f"https://api.hashify.net/hash/md4/hex?value={claim_id}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        digest = response.json().get("Digest")
        if not digest:
            logger.warning(f'No digest found in response for claim_id {claim_id}')
        return digest
    except Exception as e:
        logger.error(f'Error fetching hash for claim_id {claim_id}: {e}')
        return None


def add_hash_id(spark, df):
    # Collect unique claims_ids and get hashes
    unique_claim_ids = [row.claim_id for row in df.select("claim_id").distinct().collect()]

    # Create mapping claim_id -> hash_id
    claim_hash_map = {cid: get_md4_hash(cid) for cid in unique_claim_ids}

    logger.info(f'Successfully collected {len(unique_claim_ids)} hashes for claim ids')

    # Broadcast the mapping for use in Spark
    broadcast_hash_map = spark.sparkContext.broadcast(claim_hash_map)

    # Define UDF to get hash from broadcasted map
    def lookup_hash(claim_id):
        return broadcast_hash_map.value.get(claim_id)

    lookup_hash_udf = udf(lookup_hash, StringType())
    df = df.withColumn("hash_id", lookup_hash_udf(col("claim_id")))

    return df
