"""
Utilities for loading event sequences from Spark/Parquet format.
Converts Spark DataFrames to Python lists for preprocessing.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import config


def load_sequences_from_spark(spark: SparkSession, input_path: str = None):
    """
    Load event sequences from Parquet and convert to Python format.
    
    Args:
        spark: SparkSession
        input_path: Path to sequences Parquet file
    
    Returns:
        Tuple of (sequences, block_ids, timestamps, metadata)
        - sequences: List of lists of EventIds
        - block_ids: List of BlockIds
        - timestamps: List of lists of timestamps
        - metadata: Dict with sequence statistics
    """
    if input_path is None:
        input_path = config.SEQUENCES_PATH
    
    print(f"\n=== Loading sequences from {input_path} ===")
    
    # Load from Parquet
    df = spark.read.parquet(input_path)
    
    # Collect sequences (this brings data to driver)
    # For large datasets, we might need to sample or process in batches
    print("  Collecting sequences from Spark...")
    rows = df.collect()
    
    sequences = []
    block_ids = []
    timestamps = []
    sequence_lengths = []
    
    for row in rows:
        # Convert Spark array to Python list
        event_seq = row["EventSequence"]
        if event_seq:
            sequences.append(list(event_seq))
            block_ids.append(row["BlockId"])
            
            # Timestamps (if available)
            if "Timestamps" in row and row["Timestamps"]:
                timestamps.append(list(row["Timestamps"]))
            else:
                timestamps.append(None)
            
            # Metadata
            if "SequenceLength" in row:
                sequence_lengths.append(row["SequenceLength"])
    
    # Calculate statistics
    metadata = {
        "total_sequences": len(sequences),
        "avg_length": sum(sequence_lengths) / len(sequence_lengths) if sequence_lengths else 0,
        "min_length": min(sequence_lengths) if sequence_lengths else 0,
        "max_length": max(sequence_lengths) if sequence_lengths else 0,
    }
    
    print(f"  ✓ Loaded {len(sequences):,} sequences")
    print(f"    - Average length: {metadata['avg_length']:.2f}")
    print(f"    - Length range: {metadata['min_length']} - {metadata['max_length']}")
    
    return sequences, block_ids, timestamps, metadata


def filter_sequences_by_length(
    sequences: list,
    block_ids: list,
    timestamps: list,
    min_length: int = 1,
    max_length: int = None
):
    """
    Filter sequences by length.
    
    Args:
        sequences: List of sequences
        block_ids: List of block IDs
        timestamps: List of timestamp arrays
        min_length: Minimum sequence length
        max_length: Maximum sequence length (None = no limit)
    
    Returns:
        Filtered (sequences, block_ids, timestamps)
    """
    filtered_seq = []
    filtered_ids = []
    filtered_times = []
    
    for seq, bid, ts in zip(sequences, block_ids, timestamps):
        length = len(seq)
        if length >= min_length:
            if max_length is None or length <= max_length:
                filtered_seq.append(seq)
                filtered_ids.append(bid)
                filtered_times.append(ts)
    
    print(f"  ✓ Filtered to {len(filtered_seq):,} sequences")
    print(f"    (min_length={min_length}, max_length={max_length or 'None'})")
    
    return filtered_seq, filtered_ids, filtered_times

