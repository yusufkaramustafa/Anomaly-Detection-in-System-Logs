"""
Step 5: Visualize anomaly detection results.

This step:
1. Loads trained model and test data
2. Computes anomaly scores for test sequences
3. Creates visualizations (score distribution, statistics)
4. Analyzes top anomalies
5. Exports results to CSV
"""

import os
import torch
import numpy as np

from steps.preprocess import load_preprocessed_data
from steps.train_model import load_trained_model
from src.visualization import (
    create_visualization_summary,
    analyze_top_anomalies,
    export_anomaly_results
)
import config


def get_test_sequences():
    """
    Load test sequences as numpy arrays for visualization.
    
    Returns:
        List of numpy arrays (tokenized sequences)
    """
    _, _, test_loader, _, _ = load_preprocessed_data()
    
    test_sequences = []
    for batch in test_loader:
        batch_np = batch.cpu().numpy()
        for seq in batch_np:
            test_sequences.append(seq)
    
    return test_sequences


def run_visualize(force_recompute: bool = False, top_k: int = 50):
    """
    Execute Step 5: Visualize anomaly detection results.
    
    Args:
        force_recompute: If True, recompute scores even if visualizations exist
        top_k: Number of top anomalies to analyze in detail
    
    Returns:
        Dictionary with visualization paths and statistics
    """
    print("=" * 70)
    print("Step 5: Visualize Anomaly Detection Results")
    print("=" * 70)
    
    # Check if visualizations already exist
    viz_dir = config.VISUALIZATION_OUTPUT_DIR
    if not force_recompute and os.path.exists(viz_dir):
        viz_files = os.listdir(viz_dir)
        if any(f.endswith('.png') for f in viz_files):
            print(f"\nFound existing visualizations in {viz_dir}")
            print("Use --force-recompute to regenerate")
            return
    
    # Setup device
    if torch.cuda.is_available():
        device = torch.device('cuda')
    elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
        device = torch.device('mps')
    else:
        device = torch.device('cpu')
    
    print(f"\nUsing device: {device}")
    
    # Step 1: Load model and data
    print("\n[1/5] Loading model and test data...")
    model, _, _ = load_trained_model(device)
    _, _, test_loader, tokenizer, _ = load_preprocessed_data()
    
    # Step 2: Compute anomaly scores
    print("\n[2/5] Computing anomaly scores...")
    from src.anomaly_detector import compute_anomaly_scores, detect_anomalies
    
    anomaly_scores = compute_anomaly_scores(model, test_loader, device)
    anomaly_labels, threshold = detect_anomalies(
        anomaly_scores,
        percentile=config.ANOMALY_THRESHOLD_PERCENTILE
    )
    
    print(f"  ✓ Computed scores for {len(anomaly_scores):,} sequences")
    print(f"  ✓ Detected {anomaly_labels.sum():,} anomalies ({anomaly_labels.sum() / len(anomaly_labels) * 100:.2f}%)")
    
    # Step 3: Load test sequences for analysis
    print("\n[3/5] Loading test sequences for analysis...")
    test_sequences = get_test_sequences()
    print(f"  ✓ Loaded {len(test_sequences):,} test sequences")
    
    # Step 4: Create visualizations
    print("\n[4/5] Creating visualizations...")
    os.makedirs(viz_dir, exist_ok=True)
    
    create_visualization_summary(
        anomaly_scores=anomaly_scores,
        anomaly_labels=anomaly_labels,
        threshold=threshold,
        output_dir=viz_dir
    )
    
    # Step 5: Analyze top anomalies
    print(f"\n[5/5] Analyzing top {top_k} anomalies...")
    
    top_anomalies_path = os.path.join(viz_dir, f"top_{top_k}_anomalies.csv")
    top_anomalies_df = analyze_top_anomalies(
        anomaly_scores=anomaly_scores,
        test_sequences=test_sequences,
        tokenizer=tokenizer,
        top_k=top_k,
        output_path=top_anomalies_path
    )
    
    # Display top 10 anomalies
    print(f"\n  Top 10 Anomalies:")
    print("  " + "-" * 68)
    for _, row in top_anomalies_df.head(10).iterrows():
        seq_preview = row['Event_Sequence'][:60] + "..." if len(row['Event_Sequence']) > 60 else row['Event_Sequence']
        print(f"  Rank {row['Rank']:2d}: Score={row['Score']:.6f} | {seq_preview}")
    
    # Export comprehensive results
    export_path = os.path.join(viz_dir, "anomaly_results.csv")
    export_anomaly_results(
        anomaly_scores=anomaly_scores,
        anomaly_labels=anomaly_labels,
        test_sequences=test_sequences,
        tokenizer=tokenizer,
        output_path=export_path,
        top_k=top_k
    )
    
    # Summary
    print("\n" + "=" * 70)
    print("Visualization Summary")
    print("=" * 70)
    print(f"  • Visualizations saved to: {viz_dir}")
    print(f"  • Top {top_k} anomalies analysis: {top_anomalies_path}")
    print(f"  • Full results export: {export_path}")
    print(f"  • Total anomalies detected: {anomaly_labels.sum():,} ({anomaly_labels.sum() / len(anomaly_labels) * 100:.2f}%)")
    print("=" * 70)
    print("✓ Step 5 completed successfully!")
    print("=" * 70)
    
    return {
        'visualization_dir': viz_dir,
        'top_anomalies_path': top_anomalies_path,
        'export_path': export_path,
        'num_anomalies': int(anomaly_labels.sum()),
        'anomaly_rate': float(anomaly_labels.sum() / len(anomaly_labels) * 100)
    }
