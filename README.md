# Unsupervised Anomaly Detection in HDFS System Logs

An end-to-end pipeline for detecting anomalies in Hadoop Distributed File System (HDFS) logs using distributed processing and deep learning.

## Overview

This project combines Apache Spark for scalable log processing with LSTM autoencoders for sequence-based anomaly detection. The system identifies anomalous block-level execution traces without requiring labeled training data.

## Key Features

- **Distributed Log Processing**: Parses 11M+ log entries using Apache Spark
- **Template Matching**: Abstracts raw log messages into 29 semantic event types
- **LSTM Autoencoder**: Learns normal execution patterns through sequence reconstruction
- **Threshold Sensitivity Analysis**: Evaluates precision-recall trade-offs across operating points
- **Reproducible Pipeline**: End-to-end workflow from raw logs to anomaly scores

## Dataset

- **Source**: HDFS_V1 from LogHub
- **Size**: 11,175,629 log entries (1.47 GB)
- **Traces**: 575,061 unique block-level sequences
- **Anomaly Rate**: 2.93% (class-imbalanced)

## Requirements

Install dependencies:
```bash
pip install -r requirements.txt
```

## Quick Start

1. **Prepare data** (ensure HDFS.log and HDFS_templates.csv are in `data/` directory)

2. **Run the full pipeline**:
   ```bash
   python run.py
   ```
   This executes: parsing → sequence construction → preprocessing → model training → evaluation

3. **View results**:
   - Evaluation metrics: `models/evaluation_results.json`
   - Anomaly predictions: `results/visualizations/`
   - Benchmark comparisons: `results/benchmark/`



## Configuration

Edit `config.py` to modify:
- Data paths and Spark settings
- Model hyperparameters (embedding_dim, hidden_dim, latent_dim)
- Training settings (batch_size, epochs, learning_rate)
- Sequence length and vocabulary size


## References

- [LogHub Dataset](https://github.com/logpai/loghub) - Benchmark logs for analysis

