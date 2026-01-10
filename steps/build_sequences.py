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
from src.performance import PerformanceTracker
import config


def run_build_sequences(force_rebuild=False, min_sequence_length=1):
    """
    Execute Step 2: Build event sequences by block ID.
    
    Args:
        force_rebuild: If True, rebuild even if sequences exist
        min_sequence_length: Minimum sequence length to keep
    
    Returns:
        DataFrame with event sequences
    """
    spark = None
    with PerformanceTracker("build_sequences", config.PERFORMANCE_OUTPUT_DIR) as tracker:
        try:
            print("=" * 70)
            print("Step 2: Build Event Sequences by Block ID")
            print("=" * 70)
            
            with tracker.track_substep("create_spark_session"):
                spark = create_spark_session()
                print("✓ Spark session created\n")
            
            # Check if sequences already exist
            if not force_rebuild and os.path.exists(config.SEQUENCES_PATH):
                print("Found existing event sequences. Loading from disk...")
                with tracker.track_substep("load_existing_sequences"):
                    df_sequences = load_sequences(spark)
                    row_count = df_sequences.count()
                    tracker.record_data_size("sequences", row_count=row_count)
                    tracker.record_file_size("sequences_directory", config.SEQUENCES_PATH)
                return df_sequences
            
            print("Building event sequences from parsed logs...")
            
            # Record input data size
            tracker.record_file_size("input_parsed_logs", config.PARSED_LOGS_PATH)
            
            # Step 1: Load parsed logs
            print("\n[1/3] Loading parsed logs...")
            with tracker.track_substep("load_parsed_logs"):
                df_parsed = load_parsed_logs(spark)
                row_count = df_parsed.count()
                print(f"  ✓ Loaded {row_count:,} rows")
                tracker.record_data_size("input_parsed_logs", row_count=row_count)
            
            # Step 2: Build sequences (most efficient method)
            print("[2/3] Building event sequences by block ID...")
            with tracker.track_substep("build_sequences"):
                df_sequences = build_sequences_optimized(df_parsed)
            
            # Step 3: Add metadata and filter
            print("[3/3] Adding metadata and filtering sequences...")
            with tracker.track_substep("add_metadata_and_filter"):
                df_sequences = add_sequence_metadata(df_sequences)
                df_sequences = filter_sequences_by_length(df_sequences, min_length=min_sequence_length)
            
            # Save sequences
            with tracker.track_substep("save_sequences"):
                save_sequences(df_sequences)
                tracker.record_file_size("sequences_directory", config.SEQUENCES_PATH)
            
            # Display results
            print("\n" + "=" * 70)
            print("Sequence Building Results")
            print("=" * 70)
            
            print("\n Sample Event Sequences:")
            df_sequences.select(
                "BlockId", 
                "SequenceLength", 
                "UniqueEvents",
                "EventSequence"
            ).show(20, truncate=False)
            
            # Statistics
            with tracker.track_substep("compute_statistics"):
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
                
                tracker.record_data_size("sequences", row_count=total_sequences)
                tracker.record_data_size("total_events_in_sequences", row_count=total_events)
            
            print("\n" + "─" * 70)
            print("Summary:")
            print(f"  • Total sequences: {total_sequences:,}")
            print(f"  • Total events in sequences: {total_events:,}")
            print(f"  • Average sequence length: {avg_length:.2f}")
            print(f"  • Min sequence length: {min_length}")
            print(f"  • Max sequence length: {max_length}")
            print("─" * 70)
            
            # Sequence length distribution
            print("\n Sequence Length Distribution:")
            df_sequences.groupBy("SequenceLength") \
                .count() \
                .orderBy("SequenceLength") \
                .show(20)
            
            print("\n" + "=" * 70)
            print("✓ Step 2 completed successfully!")
            print("=" * 70)
            print(f"\n Sequences saved to: {config.SEQUENCES_PATH}")
            print("   Ready for Step 3: Model Training")
            
            # Print performance summary
            tracker.print_summary()
            
            return df_sequences
            
        except Exception as e:
            print(f"\n Error in Step 2: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
        
        finally:
            if spark:
                stop_spark_session(spark)
