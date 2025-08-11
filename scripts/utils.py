import shutil
from pathlib import Path

from config import logger


def save_single_csv(df, output_filename):
    """
    Save dataframe as single CSV without Spark's extra files.

    Args:
        df: Spark dataframe to save.
        output_filename: Destination output file path for the CSV.
    """
    temp_dir = "tmp_output"
    final_file = output_filename

    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(temp_dir))

    part_file = next(Path(temp_dir).glob("part-*.csv"))
    shutil.move(str(part_file), str(final_file))

    shutil.rmtree(temp_dir)

    logger.info(f'Saved processed data to {final_file}')
