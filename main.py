import sys
from pyspark.sql import SparkSession

from config import logger, settings
from scripts.hash_utils import add_hash_id
from scripts.transformations import add_claim_type, add_claim_priority, add_claim_period, add_source_system_id
from scripts.utils import save_single_csv


def main():
    logger.info('Starting data transformation pipeline')

    input_claims_file = settings.INPUT_CLAIMS_CSV
    input_policyholder_file = settings.INPUT_POLICYHOLDER_CSV
    output_processed_claims_file = settings.OUTPUT_PROCESSED_CLAIMS_CSV

    logger.info(f'Reading claims file: {input_claims_file}')
    logger.info(f'Reading policyholder file: {input_policyholder_file}')

    # Create SparkSession
    spark = SparkSession.builder.master("local[*]").appName("DataTransformPipeline").getOrCreate()

    # Load datasets
    claims_df = spark.read.csv(input_claims_file, header=True, inferSchema=True)
    policies_df = spark.read.csv(input_policyholder_file, header=True, inferSchema=True)

    claims_df = claims_df.withColumnRenamed("region", "claim_region")

    # Apply transformations
    processed_df = claims_df.join(policies_df, on="policyholder_id", how="inner")

    processed_df = add_claim_type(processed_df)
    processed_df = add_claim_priority(processed_df)
    processed_df = add_claim_period(processed_df)
    processed_df = add_source_system_id(processed_df)
    processed_df = add_hash_id(spark, processed_df)

    processed_df = processed_df.drop("region")
    processed_df = processed_df.withColumnRenamed("claim_region", "region")

    desired_order = ["claim_id", "policyholder_name", "region", "claim_type", "claim_priority", "claim_amount", "claim_period", "source_system_id", "hash_id"]
    processed_df = processed_df.select(*desired_order)

    processed_df.show()

    # Save the result into CSV file
    logger.info(f'Writing processed data to {output_processed_claims_file}')
    save_single_csv(processed_df, settings.OUTPUT_PROCESSED_CLAIMS_CSV)

    spark.stop()

    logger.info('Pipeline finished successfully')


if __name__ == '__main__':
    try:
        main()
    except Exception as main_exception:
        logger.exception(f'An error occurred: {main_exception}. Exiting with status code 1 ...')
        sys.exit(1)
