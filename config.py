"""
Configuration file for HDFS Anomaly Detection project.
Contains all paths, settings, and Spark configurations.
"""

# Data paths
LOG_PATH = "data/HDFS.log"
TEMPLATE_PATH = "data/HDFS_templates.csv"
PARSED_LOGS_PATH = "data/parsed_logs"
SEQUENCES_PATH = "data/event_sequences"

# Spark configuration
SPARK_CONFIG = {
    "appName": "HDFS_Anomaly_Detection",
    "spark.sql.shuffle.partitions": "200",
    "spark.executor.memory": "4g",
    "spark.driver.memory": "4g",
    "spark.sql.adaptive.enabled": "true",  # Enable adaptive query execution
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB - optimize partition size
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",  # 128MB
    "spark.memory.fraction": "0.8",  # Use 80% of heap for execution/storage
    "spark.memory.storageFraction": "0.3",  # 30% for storage, 70% for execution
}

# Log parsing settings
DATE_FORMAT = "MMDDYY"
TIME_FORMAT = "HHMMSS"
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

# Block ID extraction pattern
BLOCK_ID_PATTERN = r"blk_(-?\d+)"

# Maximum number of parameters to extract per event
MAX_PARAMETERS = 3

# Preprocessing settings (Step 3)
PREPROCESSED_DATA_PATH = "data/preprocessed"
MAX_SEQUENCE_LENGTH = 100  # Pad/truncate sequences to this length
MIN_SEQUENCE_LENGTH = 2    # Filter out sequences shorter than this
MAX_SEQUENCE_LENGTH_FILTER = None  # None = no max filter, or set to int

# Data splitting
TRAIN_RATIO = 0.7
VAL_RATIO = 0.15
TEST_RATIO = 0.15

# Training settings (for DataLoaders)
BATCH_SIZE = 32
RANDOM_SEED = 42

# Vocabulary settings
MIN_EVENT_FREQ = 1  # Minimum frequency for event to be in vocabulary

