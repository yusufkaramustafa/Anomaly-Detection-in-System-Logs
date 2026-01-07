"""
Anomaly detection utilities using trained autoencoder.
"""

import torch
import numpy as np
from torch.utils.data import DataLoader
from typing import Tuple, List
from tqdm import tqdm


def compute_anomaly_scores(
    model: torch.nn.Module,
    data_loader: DataLoader,
    device: torch.device,
    use_reconstruction_error: bool = True
) -> np.ndarray:
    """
    Compute anomaly scores for sequences using reconstruction error.
    
    Args:
        model: Trained LSTM Autoencoder
        data_loader: DataLoader with sequences
        device: Device to run on
        use_reconstruction_error: If True, use reconstruction error; else use latent distance
    
    Returns:
        anomaly_scores: Array of anomaly scores (higher = more anomalous)
    """
    model.eval()
    anomaly_scores = []
    
    with torch.no_grad():
        progress_bar = tqdm(data_loader, desc="Computing anomaly scores")
        
        for batch in progress_bar:
            sequences = batch.to(device)
            
            if use_reconstruction_error:
                # Use reconstruction error as anomaly score
                scores = model.compute_reconstruction_error(sequences, reduction='none')
                anomaly_scores.extend(scores.cpu().numpy())
            else:
                # Alternative: use latent space distance
                # Encode sequences
                latent = model.encode(sequences)
                
                # Reconstruct
                reconstructed_logits, _ = model(sequences)
                
                # Compute reconstruction error per sample
                batch_size, seq_length, vocab_size = reconstructed_logits.shape
                logits_flat = reconstructed_logits.view(-1, vocab_size)
                targets_flat = sequences.view(-1)
                
                loss = torch.nn.functional.cross_entropy(
                    logits_flat,
                    targets_flat,
                    ignore_index=0,
                    reduction='none'
                )
                
                loss = loss.view(batch_size, seq_length)
                mask = (targets_flat != 0).float().view(batch_size, seq_length)
                scores = (loss * mask).sum(dim=1) / (mask.sum(dim=1) + 1e-8)
                anomaly_scores.extend(scores.cpu().numpy())
    
    return np.array(anomaly_scores)


def compute_token_losses(
    model: torch.nn.Module,
    sequences: np.ndarray,
    device: torch.device,
    batch_size: int = 64
) -> np.ndarray:
    """
    Compute per-token reconstruction loss for sequences.
    
    Args:
        model: Trained LSTM Autoencoder
        sequences: Numpy array of shape (num_sequences, seq_length)
        device: Device to run on
        batch_size: Batch size for processing
    
    Returns:
        Array of per-token losses with shape (num_sequences, seq_length)
    """
    model.eval()
    all_losses = []
    
    with torch.no_grad():
        for i in range(0, len(sequences), batch_size):
            batch = torch.LongTensor(sequences[i:i + batch_size]).to(device)
            logits, _ = model(batch)
            
            batch_size_local, seq_length, vocab_size = logits.shape
            logits_flat = logits.view(-1, vocab_size)
            targets_flat = batch.view(-1)
            
            loss = torch.nn.functional.cross_entropy(
                logits_flat,
                targets_flat,
                ignore_index=0,
                reduction='none'
            )
            
            loss = loss.view(batch_size_local, seq_length)
            all_losses.append(loss.cpu().numpy())
    
    return np.concatenate(all_losses, axis=0)


def detect_anomalies(
    anomaly_scores: np.ndarray,
    threshold: float = None,
    percentile: float = 95.0
) -> Tuple[np.ndarray, float]:
    """
    Detect anomalies based on anomaly scores.
    
    Args:
        anomaly_scores: Array of anomaly scores
        threshold: Explicit threshold (if None, use percentile)
        percentile: Percentile to use as threshold (default: 95th)
    
    Returns:
        Tuple of (anomaly_labels, threshold_used)
        - anomaly_labels: Boolean array (True = anomaly)
        - threshold_used: Threshold value used
    """
    if threshold is None:
        threshold = np.percentile(anomaly_scores, percentile)
    
    anomaly_labels = anomaly_scores > threshold
    
    return anomaly_labels, threshold


def evaluate_anomaly_detection(
    model: torch.nn.Module,
    test_loader: DataLoader,
    device: torch.device,
    threshold_percentile: float = 95.0
) -> dict:
    """
    Evaluate anomaly detection on test set.
    
    Args:
        model: Trained LSTM Autoencoder
        test_loader: Test DataLoader
        device: Device to run on
        threshold_percentile: Percentile for threshold
    
    Returns:
        Dictionary with evaluation metrics
    """
    # Compute anomaly scores
    anomaly_scores = compute_anomaly_scores(model, test_loader, device)
    
    # Detect anomalies
    anomaly_labels, threshold = detect_anomalies(
        anomaly_scores,
        percentile=threshold_percentile
    )
    
    # Statistics
    num_anomalies = anomaly_labels.sum()
    num_normal = (~anomaly_labels).sum()
    anomaly_rate = num_anomalies / len(anomaly_labels) * 100
    
    metrics = {
        'num_samples': len(anomaly_scores),
        'num_anomalies': int(num_anomalies),
        'num_normal': int(num_normal),
        'anomaly_rate': anomaly_rate,
        'threshold': float(threshold),
        'threshold_percentile': threshold_percentile,
        'mean_score': float(anomaly_scores.mean()),
        'std_score': float(anomaly_scores.std()),
        'min_score': float(anomaly_scores.min()),
        'max_score': float(anomaly_scores.max()),
        'median_score': float(np.median(anomaly_scores))
    }
    
    return metrics, anomaly_scores, anomaly_labels
