"""
Visualization utilities for anomaly detection results.
"""

import os
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Tuple, Dict
import json
import pandas as pd


def plot_anomaly_score_distribution(
    anomaly_scores: np.ndarray,
    threshold: float,
    output_path: str,
    title: str = "Anomaly Score Distribution"
):
    """
    Plot histogram of anomaly scores with threshold line.
    
    Args:
        anomaly_scores: Array of anomaly scores
        threshold: Threshold value for anomaly detection
        output_path: Path to save the plot
        title: Plot title
    """
    plt.figure(figsize=(12, 6))
    
    # Create histogram
    plt.hist(anomaly_scores, bins=100, alpha=0.7, color='steelblue', edgecolor='black')
    
    # Add threshold line
    plt.axvline(x=threshold, color='red', linestyle='--', linewidth=2, 
                label=f'Threshold ({threshold:.4f})')
    
    # Add statistics text
    mean_score = np.mean(anomaly_scores)
    median_score = np.median(anomaly_scores)
    plt.axvline(x=mean_score, color='green', linestyle=':', linewidth=1.5, 
                label=f'Mean ({mean_score:.4f})')
    plt.axvline(x=median_score, color='orange', linestyle=':', linewidth=1.5, 
                label=f'Median ({median_score:.4f})')
    
    plt.xlabel('Anomaly Score', fontsize=12)
    plt.ylabel('Frequency', fontsize=12)
    plt.title(title, fontsize=14, fontweight='bold')
    plt.legend(fontsize=10)
    plt.grid(True, alpha=0.3)
    
    # Use log scale for y-axis if needed
    if np.max(anomaly_scores) / np.min(anomaly_scores[anomaly_scores > 0]) > 1000:
        plt.yscale('log')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Saved score distribution plot to {output_path}")


def plot_score_statistics(
    anomaly_scores: np.ndarray,
    anomaly_labels: np.ndarray,
    output_path: str
):
    """
    Plot box plots comparing normal vs anomalous scores.
    
    Args:
        anomaly_scores: Array of anomaly scores
        anomaly_labels: Boolean array (True = anomaly)
        output_path: Path to save the plot
    """
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # Box plot: Normal vs Anomalous
    normal_scores = anomaly_scores[~anomaly_labels]
    anomalous_scores = anomaly_scores[anomaly_labels]
    
    data_to_plot = [normal_scores, anomalous_scores]
    labels = ['Normal', 'Anomalous']
    
    bp = axes[0].boxplot(data_to_plot, labels=labels, patch_artist=True)
    bp['boxes'][0].set_facecolor('lightblue')
    bp['boxes'][1].set_facecolor('lightcoral')
    
    axes[0].set_ylabel('Anomaly Score', fontsize=12)
    axes[0].set_title('Score Distribution: Normal vs Anomalous', fontsize=12, fontweight='bold')
    axes[0].grid(True, alpha=0.3)
    
    # Violin plot for better distribution view
    data_df = pd.DataFrame({
        'Score': np.concatenate([normal_scores, anomalous_scores]),
        'Type': ['Normal'] * len(normal_scores) + ['Anomalous'] * len(anomalous_scores)
    })
    
    sns.violinplot(data=data_df, x='Type', y='Score', ax=axes[1], palette=['lightblue', 'lightcoral'])
    axes[1].set_title('Score Distribution (Violin Plot)', fontsize=12, fontweight='bold')
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Saved score statistics plot to {output_path}")


def decode_sequence(
    sequence: np.ndarray,
    tokenizer,
    remove_padding: bool = True
) -> List[str]:
    """
    Decode a tokenized sequence back to EventIds.
    
    Args:
        sequence: Tokenized sequence (numpy array or list)
        tokenizer: EventTokenizer instance
        remove_padding: If True, remove padding tokens
    
    Returns:
        List of EventIds
    """
    event_ids = []
    for token_id in sequence:
        if isinstance(token_id, (np.integer, int)):
            event_id = tokenizer.id_to_event.get(int(token_id), "<UNK>")
            if remove_padding and event_id == "<PAD>":
                continue
            if event_id != "<PAD>":
                event_ids.append(event_id)
        else:
            event_id = tokenizer.id_to_event.get(int(token_id.item()), "<UNK>")
            if remove_padding and event_id == "<PAD>":
                continue
            if event_id != "<PAD>":
                event_ids.append(event_id)
    
    return event_ids


def analyze_top_anomalies(
    anomaly_scores: np.ndarray,
    test_sequences: List[np.ndarray],
    tokenizer,
    top_k: int = 20,
    output_path: str = None
) -> pd.DataFrame:
    """
    Analyze top K anomalies by decoding their sequences.
    
    Args:
        anomaly_scores: Array of anomaly scores
        test_sequences: List of tokenized test sequences
        tokenizer: EventTokenizer instance
        top_k: Number of top anomalies to analyze
        output_path: Optional path to save results as CSV
    
    Returns:
        DataFrame with top anomalies analysis
    """
    # Get top K anomaly indices
    top_indices = np.argsort(anomaly_scores)[-top_k:][::-1]
    
    results = []
    for idx, score_idx in enumerate(top_indices, 1):
        sequence = test_sequences[score_idx]
        decoded_seq = decode_sequence(sequence, tokenizer)
        score = anomaly_scores[score_idx]
        
        results.append({
            'Rank': idx,
            'Score': score,
            'Sequence_Length': len(decoded_seq),
            'Event_Sequence': ' -> '.join(decoded_seq),
            'First_Event': decoded_seq[0] if decoded_seq else 'N/A',
            'Last_Event': decoded_seq[-1] if decoded_seq else 'N/A'
        })
    
    df = pd.DataFrame(results)
    
    if output_path:
        df.to_csv(output_path, index=False)
        print(f"  ✓ Saved top {top_k} anomalies analysis to {output_path}")
    
    return df


def export_anomaly_results(
    anomaly_scores: np.ndarray,
    anomaly_labels: np.ndarray,
    test_sequences: List[np.ndarray],
    tokenizer,
    output_path: str,
    top_k: int = 100
):
    """
    Export comprehensive anomaly detection results.
    
    Args:
        anomaly_scores: Array of anomaly scores
        anomaly_labels: Boolean array (True = anomaly)
        test_sequences: List of tokenized test sequences
        tokenizer: EventTokenizer instance
        output_path: Path to save CSV file
        top_k: Number of top anomalies to include with decoded sequences
    """
    # Get top K anomalies
    top_indices = np.argsort(anomaly_scores)[-top_k:][::-1]
    
    results = []
    for i, (score, is_anomaly) in enumerate(zip(anomaly_scores, anomaly_labels)):
        if i in top_indices:
            sequence = test_sequences[i]
            decoded_seq = decode_sequence(sequence, tokenizer)
            event_sequence = ' -> '.join(decoded_seq)
        else:
            event_sequence = ''  # Don't decode all sequences to save space
        
        results.append({
            'Index': i,
            'Anomaly_Score': score,
            'Is_Anomaly': bool(is_anomaly),
            'Event_Sequence': event_sequence if i in top_indices else ''
        })
    
    df = pd.DataFrame(results)
    df.to_csv(output_path, index=False)
    
    print(f"  ✓ Exported {len(results):,} anomaly results to {output_path}")
    print(f"    - Top {top_k} anomalies include decoded event sequences")


def create_visualization_summary(
    anomaly_scores: np.ndarray,
    anomaly_labels: np.ndarray,
    threshold: float,
    output_dir: str
):
    """
    Create all visualizations and save to output directory.
    
    Args:
        anomaly_scores: Array of anomaly scores
        anomaly_labels: Boolean array (True = anomaly)
        threshold: Threshold value
        output_dir: Directory to save visualizations
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Plot 1: Score distribution
    dist_path = os.path.join(output_dir, "anomaly_score_distribution.png")
    plot_anomaly_score_distribution(anomaly_scores, threshold, dist_path)
    
    # Plot 2: Normal vs Anomalous comparison
    stats_path = os.path.join(output_dir, "score_statistics.png")
    plot_score_statistics(anomaly_scores, anomaly_labels, stats_path)
    
    print(f"\n  ✓ All visualizations saved to {output_dir}")

