"""
Step 2: Build event sequences by block ID.
"""

import os
from pyspark.sql.functions import col, size, array_distinct

from src.spark_utils import create_spark_session, stop_spark_session
from src.data_io import load_parsed_logs, save_sequences, load_sequences
from src.sequence_builder import (
    build_sequences_optimized,
    add_sequence_metadata,
    filter_sequences_by_length
)
import config


def run_step2(force_rebuild=False, min_sequence_length=1):
    """
    Execute Step 2: Build event sequences by block ID.
    
    Args:
        force_rebuild: If True, rebuild even if sequences exist
        min_sequence_length: Minimum sequence length to keep
    
    Returns:
        DataFrame with event sequences
    """
    spark = None
    try:
        print("=" * 70)
        print("Step 2: Build Event Sequences by Block ID")
        print("=" * 70)
        spark = create_spark_session()
        print("✓ Spark session created\n")
        
        # Check if sequences already exist
        if not force_rebuild and os.path.exists(config.SEQUENCES_PATH):
            print("Found existing event sequences. Loading from disk...")
            return load_sequences(spark)
        
        print("Building event sequences from parsed logs...")
        
        # Step 1: Load parsed logs
        print("\n[1/3] Loading parsed logs...")
        df_parsed = load_parsed_logs(spark)
        
        # Don't cache the full dataset - it's too large (11M+ rows)
        # Spark will read from Parquet efficiently when needed
        print(f"  ✓ Loaded {df_parsed.count():,} rows")
        
        # Step 2: Build sequences (most efficient method)
        print("[2/3] Building event sequences by block ID...")
        df_sequences = build_sequences_optimized(df_parsed)
        
        # Step 3: Add metadata and filter
        print("[3/3] Adding metadata and filtering sequences...")
        df_sequences = add_sequence_metadata(df_sequences)
        df_sequences = filter_sequences_by_length(df_sequences, min_length=min_sequence_length)
        
        # Save sequences
        save_sequences(df_sequences)
        
        # Display results
        print("\n" + "=" * 70)
        print("Sequence Building Results")
        print("=" * 70)
        
        print("\n📊 Sample Event Sequences:")
        df_sequences.select(
            "BlockId", 
            "SequenceLength", 
            "UniqueEvents",
            "EventSequence"
        ).show(20, truncate=False)
        
        # Statistics
        total_sequences = df_sequences.count()
        avg_length = df_sequences.select(
            col("SequenceLength").alias("length")
        ).agg({"length": "avg"}).collect()[0][0]
        
        max_length = df_sequences.select(
            col("SequenceLength").alias("length")
        ).agg({"length": "max"}).collect()[0][0]
        
        min_length = df_sequences.select(
            col("SequenceLength").alias("length")
        ).agg({"length": "min"}).collect()[0][0]
        
        total_events = df_sequences.select(
            col("SequenceLength").alias("length")
        ).agg({"length": "sum"}).collect()[0][0]
        
        print("\n" + "─" * 70)
        print("Summary:")
        print(f"  • Total sequences: {total_sequences:,}")
        print(f"  • Total events in sequences: {total_events:,}")
        print(f"  • Average sequence length: {avg_length:.2f}")
        print(f"  • Min sequence length: {min_length}")
        print(f"  • Max sequence length: {max_length}")
        print("─" * 70)
        
        # Sequence length distribution
        print("\n📈 Sequence Length Distribution:")
        df_sequences.groupBy("SequenceLength") \
            .count() \
            .orderBy("SequenceLength") \
            .show(20)
        
        print("\n" + "=" * 70)
        print("✓ Step 2 completed successfully!")
        print("=" * 70)
        print(f"\n💾 Sequences saved to: {config.SEQUENCES_PATH}")
        print("   Ready for Step 3: Model Training")
        
        return df_sequences
        
    except Exception as e:
        print(f"\n❌ Error in Step 2: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        if spark:
            stop_spark_session(spark)

