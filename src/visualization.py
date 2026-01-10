"""
Visualization utilities for anomaly detection results.
"""

import os
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Tuple, Dict, Optional
import json
import pandas as pd
from src.evaluation import _normalize_block_id

# Set global style
sns.set_theme(style="whitegrid", context="notebook")
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['axes.spines.top'] = False
plt.rcParams['axes.spines.right'] = False


def plot_anomaly_score_distribution(
    anomaly_scores: np.ndarray,
    threshold: float,
    output_path: str,
    title: str = "Anomaly Score Distribution"
):
    """
    Plot histogram of anomaly scores with threshold line.
    """
    plt.figure(figsize=(10, 6))
    
    # Use seaborn histplot for better aesthetics
    sns.histplot(
        anomaly_scores, 
        bins=100, 
        kde=False, 
        color='#4C72B0', 
        edgecolor='white', 
        linewidth=0.5,
        alpha=0.8
    )
    
    # Add threshold line with distinct style
    plt.axvline(x=threshold, color='#C44E52', linestyle='--', linewidth=2, 
                label=f'Threshold ({threshold:.4f})')
    
    # Add statistics text
    mean_score = np.mean(anomaly_scores)
    median_score = np.median(anomaly_scores)
    
    plt.axvline(x=mean_score, color='#55A868', linestyle=':', linewidth=2, 
                label=f'Mean ({mean_score:.4f})')
    plt.axvline(x=median_score, color='#CCB974', linestyle=':', linewidth=2, 
                label=f'Median ({median_score:.4f})')
    
    plt.xlabel('Anomaly Score', fontsize=11)
    plt.ylabel('Frequency', fontsize=11)
    plt.title(title, fontsize=14, pad=15, weight='bold')
    plt.legend(frameon=True, fancybox=True, framealpha=0.9)
    
    # Smart log scale
    if np.max(anomaly_scores) > 0:
        ratio = np.max(anomaly_scores) / (np.min(anomaly_scores[anomaly_scores > 0]) + 1e-9)
        if ratio > 1000:
            plt.yscale('log')
            plt.ylabel('Frequency (Log Scale)', fontsize=11)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight', transparent=True)
    plt.close()
    
    print(f"  ✓ Saved score distribution plot to {output_path}")


def plot_score_statistics(
    anomaly_scores: np.ndarray,
    anomaly_labels: np.ndarray,
    output_path: str
):
    """
    Plot box plots comparing normal vs anomalous scores.
    """
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # Data prep
    normal_scores = anomaly_scores[~anomaly_labels]
    anomalous_scores = anomaly_scores[anomaly_labels]
    
    data_df = pd.DataFrame({
        'Score': np.concatenate([normal_scores, anomalous_scores]),
        'Type': ['Normal'] * len(normal_scores) + ['Anomalous'] * len(anomalous_scores)
    })
    
    # Define palette
    palette = {'Normal': '#4C72B0', 'Anomalous': '#C44E52'}
    
    # 1. Box Plot
    sns.boxplot(
        data=data_df, x='Type', y='Score', palette=palette, ax=axes[0],
        linewidth=1.5, flierprops={"marker": "o", "markersize": 3, "alpha": 0.5}
    )
    axes[0].set_title('Score Distribution (Box Plot)', fontsize=12, weight='bold')
    axes[0].set_xlabel('')
    axes[0].set_ylabel('Anomaly Score')

    # 2. Violin Plot
    sns.violinplot(
        data=data_df, x='Type', y='Score', palette=palette, ax=axes[1],
        inner="quartile", linewidth=1.5, alpha=0.7
    )
    axes[1].set_title('Score Density (Violin Plot)', fontsize=12, weight='bold')
    axes[1].set_xlabel('')
    axes[1].set_ylabel('')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Saved score statistics plot to {output_path}")


def plot_score_by_class(
    scores: np.ndarray,
    labels: np.ndarray,
    output_path: str
):
    """
    Plot score distributions by ground-truth class.
    """
    class_names = np.where(labels == 1, "Anomaly", "Normal")
    df = pd.DataFrame({"Score": scores, "Class": class_names})
    
    plt.figure(figsize=(10, 6))
    
    # KDE plot looks smoother than histograms for distributions
    sns.kdeplot(
        data=df,
        x="Score",
        hue="Class",
        fill=True,
        common_norm=False,
        palette={'Normal': '#4C72B0', 'Anomaly': '#C44E52'},
        alpha=0.4,
        linewidth=2
    )
    
    plt.title("Score Density by Ground-Truth Class", fontsize=14, weight="bold", pad=15)
    plt.xlabel("Anomaly Score", fontsize=11)
    plt.ylabel("Density", fontsize=11)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    
    print(f"  ✓ Saved class score distribution plot to {output_path}")


def plot_threshold_sweep(
    sweep_results: Dict[str, List[float]],
    output_path: str
):
    """
    Plot precision/recall/F1 over threshold percentiles.
    """
    percentiles = sweep_results["percentiles"]
    precision = sweep_results["precision"]
    recall = sweep_results["recall"]
    f1 = sweep_results["f1"]
    
    plt.figure(figsize=(10, 6))
    
    plt.plot(percentiles, precision, label="Precision", linewidth=2.5, color='#4C72B0')
    plt.plot(percentiles, recall, label="Recall", linewidth=2.5, color='#55A868')
    plt.plot(percentiles, f1, label="F1 Score", linewidth=2.5, color='#C44E52', linestyle='--')
    
    # Highlight max F1
    max_f1_idx = np.argmax(f1)
    plt.plot(percentiles[max_f1_idx], f1[max_f1_idx], 'o', color='#C44E52', markersize=8)
    plt.annotate(f'Max F1: {f1[max_f1_idx]:.3f}', 
                 xy=(percentiles[max_f1_idx], f1[max_f1_idx]),
                 xytext=(10, 10), textcoords='offset points',
                 fontsize=10, color='#C44E52', weight='bold')

    plt.xlabel("Threshold Percentile", fontsize=11)
    plt.ylabel("Metric Value", fontsize=11)
    plt.title("Threshold Sweep Analysis", fontsize=14, weight="bold", pad=15)
    plt.legend(frameon=True)
    plt.xlim(min(percentiles), max(percentiles))
    plt.ylim(0, 1.05)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight", transparent=True)
    plt.close()
    
    print(f"  ✓ Saved threshold sweep plot to {output_path}")


def export_false_predictions(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    scores: np.ndarray,
    output_path: str
):
    """
    Export false positives and false negatives with scores.
    
    Args:
        y_true: Ground-truth labels (0/1)
        y_pred: Predicted labels (0/1)
        scores: Anomaly scores aligned with y_true/y_pred
        output_path: Path to save CSV file
    """
    is_fp = (y_pred == 1) & (y_true == 0)
    is_fn = (y_pred == 0) & (y_true == 1)
    mask = is_fp | is_fn
    
    if not mask.any():
        return
    
    df = pd.DataFrame({
        "Prediction": np.where(y_pred[mask] == 1, "Anomaly", "Normal"),
        "Actual_Label": np.where(y_true[mask] == 1, "Anomaly", "Normal"),
        "Reconstruction_Error": scores[mask]
    })
    
    df.to_csv(output_path, index=False)
    print(f"  ✓ Saved false prediction details to {output_path}")


def plot_training_history(history: Dict[str, List[float]], output_path: str):
    """
    Plot training/validation loss history.
    """
    train_loss = history.get("train_loss", [])
    val_loss = history.get("val_loss", [])
    if not train_loss:
        return
    
    epochs = range(1, len(train_loss) + 1)
    
    plt.figure(figsize=(10, 6))
    plt.plot(epochs, train_loss, label="Training Loss", linewidth=2, color='#4C72B0')
    
    if val_loss:
        plt.plot(epochs, val_loss, label="Validation Loss", linewidth=2, color='#DD8452')
    
    plt.xlabel("Epoch", fontsize=11)
    plt.ylabel("Loss", fontsize=11)
    plt.title("Training History", fontsize=14, weight="bold", pad=15)
    plt.legend(frameon=True)
    
    # Use log scale if initial loss is massive compared to final loss
    if max(train_loss) / (min(train_loss) + 1e-9) > 100:
        plt.yscale('log')
        plt.ylabel("Loss (Log Scale)")

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    
    print(f"  ✓ Saved training history plot to {output_path}")


def plot_sequence_length_distribution(lengths: List[int], output_path: str):
    """
    Plot distribution of sequence lengths.
    """
    if not lengths:
        return
    
    plt.figure(figsize=(10, 6))
    sns.histplot(lengths, bins=60, color="#8172B3", edgecolor='white', linewidth=0.5, alpha=0.8)
    
    plt.xlabel("Sequence Length", fontsize=11)
    plt.ylabel("Frequency", fontsize=11)
    plt.title("Sequence Length Distribution", fontsize=14, weight="bold", pad=15)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    
    print(f"  ✓ Saved sequence length distribution to {output_path}")


def plot_dashboard(
    anomaly_scores: np.ndarray,
    anomaly_labels: np.ndarray,
    sequence_lengths: Optional[List[int]],
    history: Optional[Dict[str, List[float]]],
    output_path: str
):
    """
    Create a compact dashboard plot with key summaries.
    """
    fig = plt.figure(figsize=(14, 10))
    gs = fig.add_gridspec(2, 2)
    
    # Panel 1: Score distribution
    ax1 = fig.add_subplot(gs[0, 0])
    sns.histplot(anomaly_scores, bins=60, color="#4C72B0", ax=ax1, edgecolor=None, alpha=0.7)
    ax1.set_title("Score Distribution", weight='bold')
    ax1.set_xlabel("Score")
    
    # Panel 2: Anomaly ratio
    ax2 = fig.add_subplot(gs[0, 1])
    counts = pd.DataFrame({
        'Type': ['Normal', 'Anomaly'],
        'Count': [int((~anomaly_labels).sum()), int(anomaly_labels.sum())]
    })
    sns.barplot(data=counts, x='Type', y='Count', palette=['#4C72B0', '#C44E52'], ax=ax2)
    ax2.set_title("Class Balance", weight='bold')
    ax2.bar_label(ax2.containers[0], padding=3)
    
    # Panel 3: Sequence lengths
    ax3 = fig.add_subplot(gs[1, 0])
    if sequence_lengths:
        sns.histplot(sequence_lengths, bins=50, color="#8172B3", ax=ax3, edgecolor=None, alpha=0.7)
        ax3.set_title("Sequence Lengths", weight='bold')
        ax3.set_xlabel("Length")
    else:
        ax3.axis("off")
    
    # Panel 4: Training history
    ax4 = fig.add_subplot(gs[1, 1])
    if history and history.get("train_loss"):
        train_loss = history.get("train_loss", [])
        val_loss = history.get("val_loss", [])
        epochs = range(1, len(train_loss) + 1)
        ax4.plot(epochs, train_loss, label="Train", color='#4C72B0', linewidth=2)
        if val_loss:
            ax4.plot(epochs, val_loss, label="Val", color='#DD8452', linewidth=2)
        ax4.set_title("Training Loss", weight='bold')
        ax4.legend()
    else:
        ax4.axis("off")
    
    plt.suptitle("Anomaly Detection Dashboard", fontsize=16, weight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    
    print(f"  ✓ Saved dashboard plot to {output_path}")


def plot_token_loss_heatmap(
    token_losses: np.ndarray,
    decoded_sequence: List[str],
    output_path: str,
    title: Optional[str] = None
):
    """
    Plot a heatmap of per-token reconstruction loss for a sequence.
    """
    if not decoded_sequence:
        return
    
    # Cap sequence length for readability
    MAX_DISPLAY_LEN = 50
    seq_len = min(len(decoded_sequence), len(token_losses))
    display_len = min(seq_len, MAX_DISPLAY_LEN)
    
    data = np.array(token_losses[:display_len]).reshape(1, -1)
    labels = decoded_sequence[:display_len]
    
    # Calculate figure width dynamically
    fig_width = max(8, display_len * 0.4)
    plt.figure(figsize=(fig_width, 3))
    
    sns.heatmap(
        data, 
        cmap="Reds", 
        cbar_kws={'label': 'Loss'}, 
        xticklabels=labels, 
        yticklabels=False,
        square=True,
        linewidths=0.5,
        linecolor='white'
    )
    
    plt.xticks(rotation=45, ha='right', fontsize=9)
    plt.xlabel("Event Sequence (First 50 tokens)" if seq_len > MAX_DISPLAY_LEN else "Event Sequence")
    plt.title(title or "Per-Token Reconstruction Loss", fontsize=12, weight='bold', pad=10)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    
    print(f"  ✓ Saved token loss heatmap to {output_path}")


def decode_sequence(
    sequence: np.ndarray,
    tokenizer,
    remove_padding: bool = True
) -> List[str]:
    """
    Decode a tokenized sequence back to EventIds.
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
    output_path: str = None,
    token_losses: Optional[List[np.ndarray]] = None,
    block_ids: Optional[List[str]] = None,
    label_map: Optional[Dict[str, int]] = None
) -> pd.DataFrame:
    """
    Analyze top K anomalies by decoding their sequences.
    """
    top_indices = np.argsort(anomaly_scores)[-top_k:][::-1]
    
    results = []
    for idx, score_idx in enumerate(top_indices, 1):
        sequence = test_sequences[score_idx]
        decoded_seq = decode_sequence(sequence, tokenizer)
        score = anomaly_scores[score_idx]
        top_loss_events = ""
        raw_block_id = block_ids[score_idx] if block_ids else None
        label_value = None
        normalized_id = _normalize_block_id(raw_block_id) if raw_block_id else ""
        if label_map and normalized_id:
            label_value = label_map.get(normalized_id)
        
        if token_losses is not None and idx - 1 < len(token_losses):
            losses = token_losses[idx - 1]
            if losses is not None and len(losses) > 0:
                loss_seq = losses[: len(decoded_seq)]
                top_positions = np.argsort(loss_seq)[-3:][::-1]
                top_loss_events = ", ".join(
                    [decoded_seq[pos] for pos in top_positions if pos < len(decoded_seq)]
                )
        
        results.append({
            'Rank': idx,
            'Score': score,
            'Sequence_Length': len(decoded_seq),
            'Event_Sequence': ' -> '.join(decoded_seq),
            'First_Event': decoded_seq[0] if decoded_seq else 'N/A',
            'Last_Event': decoded_seq[-1] if decoded_seq else 'N/A',
            'Top_Loss_Events': top_loss_events,
            'Raw_BlockId': str(raw_block_id) if raw_block_id else '',
            'BlockId': normalized_id or '',
            'Label': 'Anomaly' if label_value == 1 else 'Normal' if label_value == 0 else 'Unknown'
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
    """
    top_indices = np.argsort(anomaly_scores)[-top_k:][::-1]
    
    results = []
    for i, (score, is_anomaly) in enumerate(zip(anomaly_scores, anomaly_labels)):
        if i in top_indices:
            sequence = test_sequences[i]
            decoded_seq = decode_sequence(sequence, tokenizer)
            event_sequence = ' -> '.join(decoded_seq)
        else:
            event_sequence = ''
        
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
    """
    os.makedirs(output_dir, exist_ok=True)
    
    dist_path = os.path.join(output_dir, "anomaly_score_distribution.png")
    plot_anomaly_score_distribution(anomaly_scores, threshold, dist_path)
    
    stats_path = os.path.join(output_dir, "score_statistics.png")
    plot_score_statistics(anomaly_scores, anomaly_labels, stats_path)
    
    print(f"\n  ✓ All visualizations saved to {output_dir}")
