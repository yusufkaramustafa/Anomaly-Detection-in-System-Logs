"""
Evaluation utilities for ground-truth labeled data.

Supports computing precision/recall/F1 and confusion matrix given
block-level labels and anomaly scores/flags.
"""

import csv
import os
from typing import Dict, List, Tuple

import numpy as np
import config


def _normalize_block_id(block_id: str) -> str:
    """
    Ensure BlockId uses the expected \"blk_\" prefix.
    
    Args:
        block_id: Raw block id string
    
    Returns:
        Normalized block id (e.g., blk_-123)
    """
    if block_id is None:
        return ""
    block_id = str(block_id).strip()
    if not block_id:
        return ""
    if block_id.startswith("blk_"):
        return block_id
    return f"blk_{block_id}"


def load_ground_truth_labels(label_path: str = None) -> Dict[str, int]:
    """
    Load ground-truth labels from CSV.
    
    Args:
        label_path: Path to CSV with columns BlockId,Label (Anomaly/Normal)
    
    Returns:
        Dict mapping BlockId -> 1 (anomaly) or 0 (normal)
    """
    label_path = label_path or config.LABEL_PATH
    labels = {}
    
    if not os.path.exists(label_path):
        raise FileNotFoundError(f"Label file not found: {label_path}")
    
    with open(label_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            label_str = row.get("Label", "").strip().lower()
            block_id = _normalize_block_id(row.get("BlockId"))
            if not block_id:
                continue
            labels[block_id] = 1 if label_str == "anomaly" else 0
    
    return labels


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    """
    Compute common classification metrics.
    
    Args:
        y_true: Ground-truth labels (0/1)
        y_pred: Predicted labels (0/1)
    
    Returns:
        Dictionary with precision, recall, f1, accuracy, and confusion matrix counts.
    """
    tp = int(((y_pred == 1) & (y_true == 1)).sum())
    fp = int(((y_pred == 1) & (y_true == 0)).sum())
    fn = int(((y_pred == 0) & (y_true == 1)).sum())
    tn = int(((y_pred == 0) & (y_true == 0)).sum())
    
    precision = tp / (tp + fp + 1e-8)
    recall = tp / (tp + fn + 1e-8)
    f1 = 2 * precision * recall / (precision + recall + 1e-8)
    accuracy = (tp + tn) / (tp + tn + fp + fn + 1e-8)
    
    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": accuracy,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
    }


def evaluate_with_labels(
    block_ids: List[str],
    anomaly_scores: np.ndarray,
    anomaly_flags: np.ndarray,
    label_path: str = None,
) -> Dict[str, float]:
    """
    Evaluate predictions against ground-truth labels.
    
    Args:
        block_ids: BlockId list aligned with anomaly_scores/anomaly_flags
        anomaly_scores: Array of anomaly scores
        anomaly_flags: Boolean/0-1 array of predicted anomalies
        label_path: Optional override for label CSV path
    
    Returns:
        Dictionary with metrics and coverage stats
    """
    labels = load_ground_truth_labels(label_path)
    
    aligned_true = []
    aligned_pred = []
    aligned_scores = []
    
    for bid, pred, score in zip(block_ids, anomaly_flags, anomaly_scores):
        norm_bid = _normalize_block_id(bid)
        if norm_bid in labels:
            aligned_true.append(labels[norm_bid])
            aligned_pred.append(int(bool(pred)))
            aligned_scores.append(float(score))
    
    if not aligned_true:
        raise ValueError("No block IDs matched the provided labels; cannot evaluate.")
    
    y_true = np.array(aligned_true)
    y_pred = np.array(aligned_pred)
    
    metrics = compute_metrics(y_true, y_pred)
    metrics.update(
        {
            "num_samples_with_labels": len(aligned_true),
            "num_labels_available": len(labels),
            "coverage_rate": len(aligned_true) / max(len(block_ids), 1),
            "positive_rate_pred": float(np.mean(y_pred)),
            "positive_rate_true": float(np.mean(y_true)),
        }
    )
    
    # Also report simple score stats for the aligned subset
    scores_arr = np.array(aligned_scores)
    metrics.update(
        {
            "score_mean": float(scores_arr.mean()),
            "score_std": float(scores_arr.std()),
            "score_min": float(scores_arr.min()),
            "score_max": float(scores_arr.max()),
            "score_median": float(np.median(scores_arr)),
        }
    )
    
    return metrics
