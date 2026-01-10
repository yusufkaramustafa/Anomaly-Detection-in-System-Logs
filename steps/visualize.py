"""
Step 5: Visualize anomaly detection results.

This step:
1. Loads trained model and test data
2. Computes anomaly scores for test sequences
3. Creates visualizations (score distribution, statistics)
4. Analyzes top anomalies
5. Exports results to CSV
"""

import json
import os
import torch
import numpy as np

from steps.preprocess import load_preprocessed_data
from steps.train_model import load_trained_model
from src.evaluation import evaluate_with_labels, get_aligned_labels, sweep_thresholds, load_ground_truth_labels
from src.visualization import (
    create_visualization_summary,
    analyze_top_anomalies,
    export_anomaly_results,
    plot_score_by_class,
    plot_threshold_sweep,
    decode_sequence,
    plot_sequence_length_distribution,
    plot_dashboard,
    export_false_predictions
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
    print("\n[1/6] Loading model and test data...")
    model, history, _ = load_trained_model(device)
    _, _, test_loader, tokenizer, _, block_ids, sequence_meta = load_preprocessed_data(
        include_block_ids=True,
        include_sequence_meta=True
    )
    
    # Step 2: Compute anomaly scores
    print("\n[2/6] Computing anomaly scores...")
    from src.anomaly_detector import compute_anomaly_scores, detect_anomalies
    
    anomaly_scores = compute_anomaly_scores(model, test_loader, device)
    
    # Apply simple metadata-based filtering to dampen noisy short/degenerate sequences
    filter_mask = None
    if sequence_meta and sequence_meta.get("test"):
        test_meta = sequence_meta["test"]
        lengths = np.array(test_meta.get("length") or [])
        uniques = np.array(test_meta.get("unique") or [])
        spans = np.array(test_meta.get("span") or [])
        
        if lengths.size and uniques.size:
            filter_mask = (lengths < config.EVAL_MIN_SEQ_LENGTH) | (
                uniques < config.EVAL_MIN_UNIQUE_EVENTS
            )
            if spans.size and config.EVAL_MIN_TIME_SPAN_SECONDS > 0:
                filter_mask |= spans < config.EVAL_MIN_TIME_SPAN_SECONDS
            
            if filter_mask.any():
                anomaly_scores = anomaly_scores.copy()
                anomaly_scores[filter_mask] = 0.0  # push filtered sequences toward normal
    
    anomaly_labels, threshold = detect_anomalies(
        anomaly_scores,
        percentile=config.ANOMALY_THRESHOLD_PERCENTILE
    )
    
    print(f"  ✓ Computed scores for {len(anomaly_scores):,} sequences")
    print(f"  ✓ Detected {anomaly_labels.sum():,} anomalies ({anomaly_labels.sum() / len(anomaly_labels) * 100:.2f}%)")
    
    # Step 3: Load test sequences for analysis
    print("\n[3/6] Loading test sequences for analysis...")
    test_sequences = get_test_sequences()
    print(f"  ✓ Loaded {len(test_sequences):,} test sequences")
    
    # Step 4: Create visualizations
    print("\n[4/6] Creating visualizations...")
    os.makedirs(viz_dir, exist_ok=True)
    
    create_visualization_summary(
        anomaly_scores=anomaly_scores,
        anomaly_labels=anomaly_labels,
        threshold=threshold,
        output_dir=viz_dir
    )

    # Sequence length distribution (if available)
    if sequence_meta and sequence_meta.get("test", {}).get("length"):
        length_path = os.path.join(viz_dir, "sequence_length_distribution.png")
        plot_sequence_length_distribution(sequence_meta["test"]["length"], length_path)

    # Dashboard
    dashboard_path = os.path.join(viz_dir, "summary_dashboard.png")
    seq_lengths = None
    if sequence_meta and sequence_meta.get("test", {}).get("length"):
        seq_lengths = sequence_meta["test"]["length"]
    plot_dashboard(anomaly_scores, anomaly_labels, seq_lengths, history, dashboard_path)
    
    # Optional: Ground-truth evaluation
    evaluation_metrics = None
    test_block_ids = None
    test_block_ids_path = os.path.join(config.PREPROCESSED_DATA_PATH, "test_block_ids.npy")
    if os.path.exists(test_block_ids_path):
        test_block_ids = np.load(test_block_ids_path, allow_pickle=True).tolist()
    
    if test_block_ids is not None and len(test_block_ids) == len(anomaly_scores):
        try:
            print("\n[5/5] Evaluating against ground-truth labels...")
            evaluation_metrics = evaluate_with_labels(
                test_block_ids,
                anomaly_scores,
                anomaly_labels,
                label_path=config.LABEL_PATH
            )
            
            os.makedirs(config.EVALUATION_OUTPUT_DIR, exist_ok=True)
            eval_path = os.path.join(config.EVALUATION_OUTPUT_DIR, "ground_truth_eval.json")
            with open(eval_path, "w") as f:
                json.dump(evaluation_metrics, f, indent=2)
            
            print(f"  ✓ Saved evaluation metrics to {eval_path}")
            print("  Evaluation:")
            print(f"    - Precision: {evaluation_metrics['precision']:.4f}")
            print(f"    - Recall:    {evaluation_metrics['recall']:.4f}")
            print(f"    - F1:        {evaluation_metrics['f1']:.4f}")
            print(f"    - Accuracy:  {evaluation_metrics['accuracy']:.4f}")
            print(f"    - Coverage:  {evaluation_metrics['coverage_rate']*100:.1f}% of test IDs matched labels")
        except Exception as eval_err:
            print(f"   Ground-truth evaluation skipped: {eval_err}")
    elif test_block_ids is None:
        print("\n[5/5] Skipping ground-truth evaluation (test_block_ids not saved)")
    else:
        print("\n[5/5] Skipping ground-truth evaluation (ID/score length mismatch)")

    # Additional labeled-data visualizations
    if test_block_ids is not None and len(test_block_ids) == len(anomaly_scores):
        try:
            y_true, aligned_scores = get_aligned_labels(
                test_block_ids,
                anomaly_scores,
                label_path=config.LABEL_PATH
            )
            class_path = os.path.join(viz_dir, "score_by_class.png")
            plot_score_by_class(aligned_scores, y_true, class_path)
            
            sweep = sweep_thresholds(y_true, aligned_scores)
            sweep_path = os.path.join(viz_dir, "threshold_sweep.png")
            plot_threshold_sweep(sweep, sweep_path)

            # False prediction details
            y_pred = (aligned_scores > threshold).astype(int)
            false_path = os.path.join(viz_dir, "false_predictions.csv")
            export_false_predictions(y_true, y_pred, aligned_scores, false_path)
        except Exception as viz_err:
            print(f"   Labeled-data plots skipped: {viz_err}")
    
    # Step 6: Analyze top anomalies
    print(f"\n[6/6] Analyzing top {top_k} anomalies...")
    
    top_anomalies_path = os.path.join(viz_dir, f"top_{top_k}_anomalies.csv")
    label_map = None
    if test_block_ids:
        try:
            label_map = load_ground_truth_labels(config.LABEL_PATH)
        except Exception:
            label_map = None

    top_anomalies_df = analyze_top_anomalies(
        anomaly_scores=anomaly_scores,
        test_sequences=test_sequences,
        tokenizer=tokenizer,
        top_k=top_k,
        output_path=top_anomalies_path,
        block_ids=test_block_ids,
        label_map=label_map
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
        'anomaly_rate': float(anomaly_labels.sum() / len(anomaly_labels) * 100),
        'evaluation_metrics': evaluation_metrics
    }
