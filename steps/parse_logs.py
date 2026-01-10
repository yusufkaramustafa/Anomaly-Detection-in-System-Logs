"""
Step 1: Ingest raw HDFS logs and parse templates with Spark.
"""

import os
from pyspark.sql.functions import col

from src.spark_utils import create_spark_session, stop_spark_session
from src.data_io import load_logs, save_parsed_logs, load_parsed_logs, get_event_statistics
from src.log_parser import parse_log_structure, convert_to_timestamp, extract_block_id
from src.template_matcher import load_and_prepare_templates, match_events, extract_parameters
from src.performance import PerformanceTracker
import config


def run_parse_logs(force_reparse=False, log_path=None, template_path=None):
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
    with PerformanceTracker("parse_logs", config.PERFORMANCE_OUTPUT_DIR) as tracker:
        try:
            print("=" * 70)
            print("Step 1: Ingest Raw HDFS Logs and Parse Templates")
            print("=" * 70)
            
            with tracker.track_substep("create_spark_session"):
                spark = create_spark_session()
                print("✓ Spark session created\n")
            
            # Check if parsed data already exists
            if not force_reparse and os.path.exists(config.PARSED_LOGS_PATH):
                print("Found existing parsed logs. Loading from disk...")
                with tracker.track_substep("load_existing_data"):
                    df_parsed = load_parsed_logs(spark)
                    row_count = df_parsed.count()
                    tracker.record_data_size("parsed_logs", row_count=row_count)
                    tracker.record_file_size("parsed_logs_directory", config.PARSED_LOGS_PATH)
                return df_parsed
            
            print("Parsing raw HDFS logs...")
            
            # Record input file size
            log_file = log_path or config.LOG_PATH
            tracker.record_file_size("input_log_file", log_file)
            
            # Step 1: Load raw logs
            print("\n[1/5] Loading raw log file...")
            with tracker.track_substep("load_raw_logs"):
                df_logs = load_logs(spark, log_file)
                row_count = df_logs.count()
                tracker.record_data_size("raw_logs", row_count=row_count)
            
            # Step 2: Parse log structure
            print("[2/5] Parsing log structure...")
            with tracker.track_substep("parse_log_structure"):
                df_parsed = parse_log_structure(df_logs)
                df_parsed = convert_to_timestamp(df_parsed)
                df_parsed = extract_block_id(df_parsed)
            
            # Step 3: Load and prepare templates
            print("[3/5] Loading event templates...")
            template_file = template_path or config.TEMPLATE_PATH
            with tracker.track_substep("load_templates"):
                templates_list = load_and_prepare_templates(spark, template_file)
                print(f"  ✓ Loaded {len(templates_list)} event templates")
                tracker.record_data_size("templates", row_count=len(templates_list))
                tracker.record_file_size("template_file", template_file)
            
            # Step 4: Match events to templates
            print("[4/5] Matching events to templates...")
            with tracker.track_substep("match_events"):
                df_parsed = match_events(df_parsed, templates_list)
            
            # Step 5: Extract parameters
            print("[5/5] Extracting parameters from matched events...")
            with tracker.track_substep("extract_parameters"):
                df_parsed = extract_parameters(df_parsed, templates_list)
            
            # Save parsed data (no need to cache - data is saved to Parquet)
            print("\nSaving parsed data...")
            with tracker.track_substep("save_parsed_data"):
                save_parsed_logs(df_parsed)
                tracker.record_file_size("parsed_logs_directory", config.PARSED_LOGS_PATH)
            
            # Display results
            print("\n" + "=" * 70)
            print("Parsing Results")
            print("=" * 70)
            
            print("\n Sample Parsed Logs:")
            df_parsed.select(
                "Timestamp", "Component", "EventId", "BlockId", "Message"
            ).show(20, truncate=False)
            
            print("\n Schema:")
            df_parsed.printSchema()
            
            print("\n Event Statistics:")
            event_stats = get_event_statistics(df_parsed)
            event_stats.show()
            
            # Summary statistics
            with tracker.track_substep("compute_statistics"):
                total_logs = df_parsed.count()
                matched_logs = df_parsed.filter(col("EventId").isNotNull()).count()
                unmatched_logs = total_logs - matched_logs
                blocks_with_events = df_parsed.filter(
                    (col("BlockId").isNotNull()) & (col("EventId").isNotNull())
                ).select("BlockId").distinct().count()
                
                tracker.record_data_size("parsed_logs", row_count=total_logs)
                tracker.record_data_size("matched_events", row_count=matched_logs)
                tracker.record_data_size("unmatched_events", row_count=unmatched_logs)
                tracker.record_data_size("unique_blocks", row_count=blocks_with_events)
              
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
            
            # Print performance summary
            tracker.print_summary()
            
            return df_parsed
            
        except Exception as e:
            print(f"\n Error in Step 1: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
        
        finally:
            if spark:
                stop_spark_session(spark)
