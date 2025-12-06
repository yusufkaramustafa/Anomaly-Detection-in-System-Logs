"""
Spark session utilities for creating and managing Spark sessions.
"""

from pyspark.sql import SparkSession
import config


def create_spark_session(config_dict=None):
    """
    Create and return a Spark session with optimized configurations.
    
    Args:
        config_dict: Optional dictionary of Spark configs. 
                     If None, uses config.SPARK_CONFIG.
    
    Returns:
        SparkSession: Configured Spark session
    """
    if config_dict is None:
        config_dict = config.SPARK_CONFIG
    
    builder = SparkSession.builder.appName(config_dict["appName"])
    
    # Add all configuration settings
    for key, value in config_dict.items():
        if key != "appName":  # Already set above
            builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    
    # Set log level to WARN to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def stop_spark_session(spark):
    """
    Stop the Spark session gracefully.
    
    Args:
        spark: SparkSession to stop
    """
    if spark:
        spark.stop()

