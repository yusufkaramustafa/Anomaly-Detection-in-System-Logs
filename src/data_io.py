"""
Data I/O utilities for loading and saving data.
"""

from pyspark.sql.functions import col
import config


def load_logs(spark, log_path=None):
    """
    Load raw log file as text.
    
    Args:
        spark: SparkSession
        log_path: Path to log file (defaults to config.LOG_PATH)
    
    Returns:
        DataFrame with log text in 'value' column
    """
    if log_path is None:
        log_path = config.LOG_PATH
    
    return spark.read.text(log_path)


def save_parsed_logs(df_parsed, output_path=None, mode="overwrite"):
    """
    Save parsed logs to Parquet format.
    
    Args:
        df_parsed: DataFrame to save
        output_path: Output path (defaults to config.PARSED_LOGS_PATH)
        mode: Write mode (default: "overwrite")
    
    Returns:
        None
    """
    if output_path is None:
        output_path = config.PARSED_LOGS_PATH
    
    print(f"\n=== Saving parsed logs to {output_path} ===")
    df_parsed.write.mode(mode).parquet(output_path)
    print(f"✓ Parsed logs saved successfully! ({df_parsed.count()} rows)")


def load_parsed_logs(spark, input_path=None):
    """
    Load previously parsed logs from Parquet format.
    
    Args:
        spark: SparkSession
        input_path: Input path (defaults to config.PARSED_LOGS_PATH)
    
    Returns:
        DataFrame with parsed logs
    """
    if input_path is None:
        input_path = config.PARSED_LOGS_PATH
    
    print(f"\n=== Loading parsed logs from {input_path} ===")
    df_parsed = spark.read.parquet(input_path)
    print(f"✓ Loaded {df_parsed.count()} rows")
    
    return df_parsed


def get_event_statistics(df_parsed):
    """
    Get statistics about event distribution.
    
    Args:
        df_parsed: DataFrame with EventId column
    
    Returns:
        DataFrame with event counts ordered by frequency
    """
    return df_parsed.filter(col("EventId").isNotNull()) \
                    .groupBy("EventId") \
                    .count() \
                    .orderBy("count", ascending=False)

