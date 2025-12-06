"""
Step 1: Ingest raw HDFS logs and parse templates with Spark.
"""

import os
from pyspark.sql.functions import col

from src.spark_utils import create_spark_session, stop_spark_session
from src.data_io import load_logs, save_parsed_logs, load_parsed_logs, get_event_statistics
from src.log_parser import parse_log_structure, convert_to_timestamp, extract_block_id
from src.template_matcher import load_and_prepare_templates, match_events, extract_parameters
import config


def run_step1(force_reparse=False, log_path=None, template_path=None):
    """
    Execute Step 1: Parse HDFS logs.
    
    Args:
        force_reparse: If True, reparse even if saved data exists
        log_path: Path to raw log file (defaults to config.LOG_PATH)
        template_path: Path to template CSV (defaults to config.TEMPLATE_PATH)
    
    Returns:
        DataFrame with parsed logs
    """
    spark = None
    try:
        print("=" * 70)
        print("Step 1: Ingest Raw HDFS Logs and Parse Templates")
        print("=" * 70)
        spark = create_spark_session()
        print("✓ Spark session created\n")
        
        # Check if parsed data already exists
        if not force_reparse and os.path.exists(config.PARSED_LOGS_PATH):
            print("Found existing parsed logs. Loading from disk...")
            return load_parsed_logs(spark)
        
        print("Parsing raw HDFS logs...")
        
        # Step 1: Load raw logs
        print("\n[1/5] Loading raw log file...")
        df_logs = load_logs(spark, log_path or config.LOG_PATH)
        
        # Step 2: Parse log structure
        print("[2/5] Parsing log structure...")
        df_parsed = parse_log_structure(df_logs)
        df_parsed = convert_to_timestamp(df_parsed)
        df_parsed = extract_block_id(df_parsed)
        
        # Step 3: Load and prepare templates
        print("[3/5] Loading event templates...")
        templates_list = load_and_prepare_templates(spark, template_path or config.TEMPLATE_PATH)
        print(f"  ✓ Loaded {len(templates_list)} event templates")
        
        # Step 4: Match events to templates
        print("[4/5] Matching events to templates...")
        df_parsed = match_events(df_parsed, templates_list)
        
        # Step 5: Extract parameters
        print("[5/5] Extracting parameters from matched events...")
        df_parsed = extract_parameters(df_parsed, templates_list)
        
        # Save parsed data (no need to cache - data is saved to Parquet)
        print("\nSaving parsed data...")
        save_parsed_logs(df_parsed)
        
        # Display results
        print("\n" + "=" * 70)
        print("Parsing Results")
        print("=" * 70)
        
        print("\n📊 Sample Parsed Logs:")
        df_parsed.select(
            "Timestamp", "Component", "EventId", "BlockId", "Message"
        ).show(20, truncate=False)
        
        print("\n📋 Schema:")
        df_parsed.printSchema()
        
        print("\n📈 Event Statistics:")
        event_stats = get_event_statistics(df_parsed)
        event_stats.show()
        
        # Summary statistics
        total_logs = df_parsed.count()
        matched_logs = df_parsed.filter(col("EventId").isNotNull()).count()
        unmatched_logs = total_logs - matched_logs
        blocks_with_events = df_parsed.filter(
            (col("BlockId").isNotNull()) & (col("EventId").isNotNull())
        ).select("BlockId").distinct().count()
        
        print("\n" + "─" * 70)
        print("Summary:")
        print(f"  • Total log entries: {total_logs:,}")
        print(f"  • Matched events: {matched_logs:,} ({matched_logs/total_logs*100:.1f}%)")
        print(f"  • Unmatched events: {unmatched_logs:,} ({unmatched_logs/total_logs*100:.1f}%)")
        print(f"  • Unique blocks with events: {blocks_with_events:,}")
        print("─" * 70)
        
        print("\n" + "=" * 70)
        print("✓ Step 1 completed successfully!")
        print("=" * 70)
        
        return df_parsed
        
    except Exception as e:
        print(f"\n❌ Error in Step 1: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        if spark:
            stop_spark_session(spark)

