"""
Efficient sequence building utilities for creating event sequences by block ID.
Uses optimized Spark operations for maximum performance.
"""

from pyspark.sql.functions import (
    col, collect_list, array_sort, struct, expr,
    row_number, window, count as spark_count,
    size, array_distinct, array_min, array_max
)
from pyspark.sql.window import Window
import config


def filter_valid_events(df_parsed):
    """
    Filter out rows with NULL BlockId or EventId.
    This early filtering reduces data size for subsequent operations.
    
    Args:
        df_parsed: DataFrame with BlockId and EventId columns
    
    Returns:
        Filtered DataFrame with only valid events
    """
    return df_parsed.filter(
        (col("BlockId").isNotNull()) & 
        (col("BlockId") != "") &
        (col("EventId").isNotNull()) &
        (col("EventId") != "")
    )


def build_sequences_optimized(df_parsed):
    """
    Build event sequences by block ID using optimized Spark operations.
    
    This method ensures events are collected in timestamp order by:
    1. Creating a struct with (Timestamp, EventId) to preserve ordering
    2. Collecting the structs
    3. Sorting the array by Timestamp
    4. Extracting EventIds in order
    
    This is more efficient than Window functions for large datasets.
    
    Args:
        df_parsed: DataFrame with BlockId, EventId, and Timestamp columns
    
    Returns:
        DataFrame with BlockId, EventSequence, and Timestamps columns
    """
    # Filter valid events first (reduces data size)
    df_valid = filter_valid_events(df_parsed)
    
    # Create struct to preserve timestamp-event pairing
    # This ensures we can sort by timestamp after collection
    df_with_struct = df_valid.select(
        "BlockId",
        struct("Timestamp", "EventId").alias("EventWithTime")
    )
    
    # Group by BlockId and collect structs
    df_collected = df_with_struct.groupBy("BlockId") \
        .agg(collect_list("EventWithTime").alias("EventsWithTime"))
    
    # Sort arrays by Timestamp and extract EventIds and Timestamps
    # array_sort sorts by the first field of the struct (Timestamp)
    df_sequences = df_collected.withColumn(
        "SortedEvents",
        array_sort(col("EventsWithTime"))
    ).withColumn(
        "EventSequence",
        expr("transform(SortedEvents, x -> x.EventId)")
    ).withColumn(
        "Timestamps",
        expr("transform(SortedEvents, x -> x.Timestamp)")
    ).select("BlockId", "EventSequence", "Timestamps")
    
    return df_sequences


def build_sequences_with_window(df_parsed):
    """
    Alternative method using Window functions.
    More memory efficient for very large datasets but potentially slower.
    
    Args:
        df_parsed: DataFrame with BlockId, EventId, and Timestamp columns
    
    Returns:
        DataFrame with BlockId and EventSequence columns
    """
    # Filter valid events
    df_valid = filter_valid_events(df_parsed)
    
    # Define window partitioned by BlockId, ordered by Timestamp
    window_spec = Window.partitionBy("BlockId").orderBy("Timestamp")
    
    # Add row number to identify sequence position
    df_with_rank = df_valid.withColumn(
        "seq_rank",
        row_number().over(window_spec)
    )
    
    # Get max rank per block to know sequence length
    max_rank_df = df_with_rank.groupBy("BlockId") \
        .agg(spark_count("*").alias("seq_length"))
    
    # Join and collect sequences
    # This approach is more complex but can be more memory efficient
    # For most cases, build_sequences_optimized is better
    df_sequences = df_with_rank.join(max_rank_df, "BlockId") \
        .groupBy("BlockId") \
        .agg(
            collect_list("EventId").alias("EventSequence"),
            collect_list("Timestamp").alias("Timestamps")
        )
    
    return df_sequences


def add_sequence_metadata(df_sequences):
    """
    Add metadata columns to sequences for analysis.
    
    Args:
        df_sequences: DataFrame with BlockId and EventSequence columns
    
    Returns:
        DataFrame with additional metadata columns
    """
    return (
        df_sequences
        .withColumn("SequenceLength", size(col("EventSequence")))
        .withColumn("UniqueEvents", size(array_distinct(col("EventSequence"))))
        .withColumn(
            "TimeSpanSeconds",
            expr(
                "CASE WHEN size(Timestamps) > 0 "
                "THEN unix_timestamp(array_max(Timestamps)) - unix_timestamp(array_min(Timestamps)) "
                "ELSE 0 END"
            )
        )
        .withColumn(
            "FirstEvent",
            expr("CASE WHEN size(EventSequence) > 0 THEN EventSequence[0] ELSE null END")
        )
        .withColumn(
            "LastEvent",
            expr(
                "CASE WHEN size(EventSequence) > 0 THEN EventSequence[size(EventSequence)-1] ELSE null END"
            )
        )
    )


def filter_sequences_by_length(df_sequences, min_length=1, max_length=None):
    """
    Filter sequences by length to remove outliers or very short sequences.
    
    Args:
        df_sequences: DataFrame with SequenceLength column
        min_length: Minimum sequence length (default: 1)
        max_length: Maximum sequence length (None = no limit)
    
    Returns:
        Filtered DataFrame
    """
    df_filtered = df_sequences.filter(col("SequenceLength") >= min_length)
    
    if max_length is not None:
        df_filtered = df_filtered.filter(col("SequenceLength") <= max_length)
    
    return df_filtered
