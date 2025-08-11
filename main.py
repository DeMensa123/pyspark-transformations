from pyspark.sql import SparkSession
import sys

from config import logger


def main():
    logger.info('Starting data transformation pipeline')

    # Create SparkSession
    spark = SparkSession.builder.master("local[*]").appName("DataTransformPipeline").getOrCreate()

    spark.stop()


if __name__ == '__main__':
    try:
        main()
    except Exception as main_exception:
        logger.exception(f'An error occurred: {main_exception}. Exiting with status code 1 ...')
        sys.exit(1)
