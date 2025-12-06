"""
Log parsing utilities for HDFS log files.
Handles parsing of log structure and timestamp conversion.
"""

from pyspark.sql.functions import (
    regexp_extract, col, to_timestamp, concat_ws, concat, lit
)
import config


def parse_log_structure(df_logs):
    """
    Parse the HDFS log structure into separate columns.
    
    Log format: MMDDYY HHMMSS ThreadID LEVEL Component: Message
    
    Args:
        df_logs: DataFrame with raw log text in 'value' column
    
    Returns:
        DataFrame with parsed columns: Date, Time, ThreadID, LogLevel, Component, Message
    """
    df_parsed = df_logs.withColumn(
        "Date", regexp_extract(col("value"), r"^(\d{6})", 1)
    ).withColumn(
        "Time", regexp_extract(col("value"), r"^\d{6} (\d{6})", 1)
    ).withColumn(
        "ThreadID", regexp_extract(col("value"), r"^\d{6} \d{6} (\d+)", 1)
    ).withColumn(
        "LogLevel", regexp_extract(col("value"), r"^\d{6} \d{6} \d+ (\w+)", 1)
    ).withColumn(
        "Component", regexp_extract(col("value"), r"^\d{6} \d{6} \d+ \w+ ([^:]+):", 1)
    ).withColumn(
        "Message", regexp_extract(col("value"), r"^\d{6} \d{6} \d+ \w+ [^:]+: (.+)$", 1)
    )
    
    return df_parsed


def convert_to_timestamp(df_parsed):
    """
    Convert Date and Time columns to a Timestamp column.
    
    Args:
        df_parsed: DataFrame with Date and Time columns (MMDDYY and HHMMSS format)
    
    Returns:
        DataFrame with Timestamp column added
    """
    # Build timestamp string: YYYY-MM-DD HH:MM:SS
    df_with_timestamp = df_parsed.withColumn(
        "TimestampStr",
        concat_ws(" ", 
            concat_ws("-", 
                concat(lit("20"), col("Date").substr(5, 2)),  # Year: 20 + YY
                col("Date").substr(1, 2),                     # Month: MM
                col("Date").substr(3, 2)                      # Day: DD
            ),
            concat_ws(":", 
                col("Time").substr(1, 2),                     # Hour: HH
                col("Time").substr(3, 2),                     # Minute: MM
                col("Time").substr(5, 2)                      # Second: SS
            )
        )
    ).withColumn(
        "Timestamp",
        to_timestamp(col("TimestampStr"), config.TIMESTAMP_FORMAT)
    ).drop("TimestampStr")
    
    return df_with_timestamp


def extract_block_id(df_parsed):
    """
    Extract block ID from log messages.
    
    Args:
        df_parsed: DataFrame with Message column
    
    Returns:
        DataFrame with BlockId column added
    """
    df_with_block = df_parsed.withColumn(
        "BlockId",
        regexp_extract(col("Message"), config.BLOCK_ID_PATTERN, 1)
    )
    
    return df_with_block

