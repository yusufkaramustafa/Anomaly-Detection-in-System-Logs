"""
Baseline anomaly scoring with Spark ML KMeans.

Approach:
- Build hashed term-frequency vectors from EventSequence
- Append simple scalar features (SequenceLength, UniqueEvents, TimeSpanSeconds)
- Fit KMeans and use distance to assigned centroid as anomaly score
"""

import os
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import HashingTF, VectorAssembler
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType

from src.spark_utils import create_spark_session, stop_spark_session
import config


def run_baseline_kmeans(
    num_features: int = 256,
    k: int = 8,
    max_iter: int = 20,
    force_recompute: bool = False
):
    """
    Fit a KMeans baseline and compute distance-based anomaly scores.
    
    Args:
        num_features: Hashing dimension for EventSequence vectors
        k: Number of clusters
        max_iter: Max iterations for KMeans
        force_recompute: If False and scores exist, skip recompute
    """
    output_dir = os.path.join("results", "baseline")
    os.makedirs(output_dir, exist_ok=True)
    scores_path = os.path.join(output_dir, "kmeans_scores.parquet")
    
    if os.path.exists(scores_path) and not force_recompute:
        print(f"Found existing baseline scores at {scores_path} (use force_recompute to rebuild)")
        return scores_path
    
    spark = create_spark_session()
    try:
        df = spark.read.parquet(config.SEQUENCES_PATH)
        required_cols = ["EventSequence", "SequenceLength", "UniqueEvents", "TimeSpanSeconds"]
        for c in required_cols:
            if c not in df.columns:
                raise ValueError(f"Column {c} missing; rerun sequence building to add metadata.")
        
        tf = HashingTF(
            inputCol="EventSequence",
            outputCol="tf_raw",
            numFeatures=num_features
        )
        assembler = VectorAssembler(
            inputCols=["tf_raw", "SequenceLength", "UniqueEvents", "TimeSpanSeconds"],
            outputCol="features",
            handleInvalid="keep"
        )
        
        df_featurized = assembler.transform(tf.transform(df))
        
        kmeans = KMeans(
            featuresCol="features",
            predictionCol="cluster",
            k=k,
            maxIter=max_iter,
            seed=config.RANDOM_SEED
        )
        
        model = kmeans.fit(df_featurized)
        centers = model.clusterCenters()
        
        # UDF to compute distance to assigned centroid
        def distance_to_center(vector, cluster_id):
            center = centers[int(cluster_id)]
            return float(vector.squared_distance(center)) ** 0.5
        
        dist_udf = udf(distance_to_center, DoubleType())
        df_scored = model.transform(df_featurized).withColumn(
            "anomaly_score", dist_udf(col("features"), col("cluster"))
        )
        
        # Persist scores
        df_scored.select(
            "BlockId",
            "anomaly_score",
            "SequenceLength",
            "UniqueEvents",
            "TimeSpanSeconds",
            "cluster"
        ).write.mode("overwrite").parquet(scores_path)
        
        print(f"✓ Saved KMeans baseline scores to {scores_path}")
        return scores_path
    finally:
        stop_spark_session(spark)
