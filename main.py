"""
Main script for Step 1: Ingest raw HDFS logs and parse templates with Spark.

This script:
1. Loads raw HDFS log files
2. Parses log structure (timestamp, component, message, etc.)
3. Matches log messages to event templates
4. Extracts block IDs and parameters
5. Saves parsed data for Step 2
"""

import os
import sys
from pyspark.sql.functions import col

# Add src directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.spark_utils import create_spark_session, stop_spark_session
from src.data_io import load_logs, save_parsed_logs, load_parsed_logs, get_event_statistics
from src.log_parser import parse_log_structure, convert_to_timestamp, extract_block_id
from src.template_matcher import load_and_prepare_templates, match_events, extract_parameters
import config


def parse_hdfs_logs(spark, log_path=None, template_path=None, force_reparse=False):
    """
    Main function to parse HDFS logs.
    
    Args:
        spark: SparkSession
        log_path: Path to raw log file
        template_path: Path to template CSV file
        force_reparse: If True, reparse even if saved data exists
    
    Returns:
        DataFrame with parsed logs
    """
    # Check if parsed data already exists
    if not force_reparse and os.path.exists(config.PARSED_LOGS_PATH):
        print("Found existing parsed logs. Loading from disk...")
        return load_parsed_logs(spark)
    
    print("Parsing raw HDFS logs...")
    
    # Step 1: Load raw logs
    print("\n[Step 1/5] Loading raw log file...")
    df_logs = load_logs(spark, log_path)
    
    # Step 2: Parse log structure
    print("[Step 2/5] Parsing log structure...")
    df_parsed = parse_log_structure(df_logs)
    df_parsed = convert_to_timestamp(df_parsed)
    df_parsed = extract_block_id(df_parsed)
    
    # Step 3: Load and prepare templates
    print("[Step 3/5] Loading event templates...")
    templates_list = load_and_prepare_templates(spark, template_path)
    print(f"  Loaded {len(templates_list)} event templates")
    
    # Step 4: Match events to templates
    print("[Step 4/5] Matching events to templates...")
    df_parsed = match_events(df_parsed, templates_list)
    
    # Step 5: Extract parameters
    print("[Step 5/5] Extracting parameters from matched events...")
    df_parsed = extract_parameters(df_parsed, templates_list)
    
    # Cache the dataframe for faster access
    print("\nCaching parsed data...")
    df_parsed.cache()
    
    # Force computation to populate cache
    _ = df_parsed.count()
    
    # Save parsed data
    save_parsed_logs(df_parsed)
    
    return df_parsed


def main():
    """Main execution function."""
    spark = None
    try:
        # Create Spark session
        print("=" * 60)
        print("HDFS Anomaly Detection - Step 1: Log Parsing")
        print("=" * 60)
        spark = create_spark_session()
        print("✓ Spark session created")
        
        # Parse logs
        df_parsed = parse_hdfs_logs(
            spark,
            log_path=config.LOG_PATH,
            template_path=config.TEMPLATE_PATH,
            force_reparse=False  # Set to True to force re-parsing
        )
        
        # Display results
        print("\n" + "=" * 60)
        print("Parsing Results")
        print("=" * 60)
        
        print("\n=== Sample Parsed Logs ===")
        df_parsed.select(
            "Timestamp", "Component", "EventId", "BlockId", "Message"
        ).show(20, truncate=False)
        
        print("\n=== Schema ===")
        df_parsed.printSchema()
        
        print("\n=== Event Statistics ===")
        event_stats = get_event_statistics(df_parsed)
        event_stats.show()
        
        # Summary statistics
        total_logs = df_parsed.count()
        matched_logs = df_parsed.filter(col("EventId").isNotNull()).count()
        unmatched_logs = total_logs - matched_logs
        blocks_with_events = df_parsed.filter(
            (col("BlockId").isNotNull()) & (col("EventId").isNotNull())
        ).select("BlockId").distinct().count()
        
        print("\n=== Summary ===")
        print(f"Total log entries: {total_logs:,}")
        print(f"Matched events: {matched_logs:,} ({matched_logs/total_logs*100:.1f}%)")
        print(f"Unmatched events: {unmatched_logs:,} ({unmatched_logs/total_logs*100:.1f}%)")
        print(f"Unique blocks with events: {blocks_with_events:,}")
        
        print("\n" + "=" * 60)
        print("Step 1 completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        # Stop Spark session
        if spark:
            stop_spark_session(spark)
            print("\n✓ Spark session stopped")


if __name__ == "__main__":
    main()
