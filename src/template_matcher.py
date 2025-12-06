"""
Template matching utilities for matching log messages to event templates.
"""

from pyspark.sql.functions import (
    regexp_replace, col, when, lit, regexp_extract
)
from pyspark.sql.types import StringType
import config


def load_and_prepare_templates(spark, template_path):
    """
    Load event templates and convert them to regex patterns.
    
    Args:
        spark: SparkSession
        template_path: Path to CSV file with EventId and EventTemplate columns
    
    Returns:
        List of dictionaries with EventId and Regex pattern
    """
    df_templates = spark.read.csv(template_path, header=True)
    
    # Convert templates into regex patterns
    df_templates = df_templates.withColumn(
        "Regex",
        regexp_replace(col("EventTemplate"), "<\\*>", "(.*)")
    )
    
    # Collect templates as list for iteration
    templates_list = df_templates.collect()
    
    return templates_list


def match_events(df_parsed, templates_list):
    """
    Match log messages to event templates and assign EventIds.
    
    Args:
        df_parsed: DataFrame with Message column
        templates_list: List of template dictionaries with EventId and Regex
    
    Returns:
        DataFrame with EventId column added/updated
    """
    # Initialize EventId column
    df_with_events = df_parsed.withColumn("EventId", lit(None).cast(StringType()))
    
    # Apply templates to match events
    for row in templates_list:
        event_id = row["EventId"]
        pattern = row["Regex"]
        
        df_with_events = df_with_events.withColumn(
            "EventId",
            when(col("Message").rlike(pattern), event_id).otherwise(col("EventId"))
        )
    
    return df_with_events


def extract_parameters(df_parsed, templates_list):
    """
    Extract parameters from matched events using regex capture groups.
    
    Args:
        df_parsed: DataFrame with EventId and Message columns
        templates_list: List of template dictionaries with EventId and Regex
    
    Returns:
        DataFrame with Parameter1, Parameter2, Parameter3 columns added
    """
    # Initialize parameter columns
    df_with_params = df_parsed.withColumn("Parameter1", lit(None).cast(StringType())) \
                              .withColumn("Parameter2", lit(None).cast(StringType())) \
                              .withColumn("Parameter3", lit(None).cast(StringType()))
    
    # Extract parameters for each matched event type
    for row in templates_list:
        eid = row["EventId"]
        pattern = row["Regex"]
        
        # Count number of capture groups in pattern
        num_params = pattern.count("(.*)")
        
        # Extract parameters only if we have enough capture groups
        if num_params >= 1:
            df_with_params = df_with_params.withColumn(
                "Parameter1",
                when((col("EventId") == eid) & (num_params >= 1), 
                     regexp_extract(col("Message"), pattern, 1))
                .otherwise(col("Parameter1"))
            )
        
        if num_params >= 2:
            df_with_params = df_with_params.withColumn(
                "Parameter2",
                when((col("EventId") == eid) & (num_params >= 2), 
                     regexp_extract(col("Message"), pattern, 2))
                .otherwise(col("Parameter2"))
            )
        
        if num_params >= 3:
            df_with_params = df_with_params.withColumn(
                "Parameter3",
                when((col("EventId") == eid) & (num_params >= 3), 
                     regexp_extract(col("Message"), pattern, 3))
                .otherwise(col("Parameter3"))
            )
    
    return df_with_params

