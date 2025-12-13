"""
Step 4: Train LSTM Autoencoder for anomaly detection.

This step:
1. Loads preprocessed data from Step 3
2. Initializes LSTM Autoencoder model
3. Trains the model on normal sequences
4. Saves the trained model
5. Evaluates on test set and computes anomaly scores
"""

import os
import torch
import torch.nn as nn
import json

from steps.step3_preprocess import load_preprocessed_data
from src.models.lstm_autoencoder import LSTMAutoencoder
from src.training import train_model
from src.anomaly_detector import evaluate_anomaly_detection
import config


def train_autoencoder(
    force_retrain: bool = False,
    num_epochs: int = None,
    learning_rate: float = None,
    device: str = None
):
    """
    Train LSTM Autoencoder for anomaly detection.
    
    Args:
        force_retrain: If True, retrain even if model exists
        num_epochs: Number of training epochs
        learning_rate: Learning rate
        device: Device to use ('cuda', 'mps', or 'cpu')
    
    Returns:
        Tuple of (model, history, test_metrics)
    """
    # Setup device
    if device is None:
        if torch.cuda.is_available():
            device = torch.device('cuda')
        elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
            device = torch.device('mps')
        else:
            device = torch.device('cpu')
    else:
        device = torch.device(device)
    
    print("=" * 70)
    print("Step 4: Train LSTM Autoencoder for Anomaly Detection")
    print("=" * 70)
    print(f"Device: {device}\n")
    
    # Check if model already exists
    if not force_retrain and os.path.exists(config.MODEL_SAVE_PATH):
        print(f"Found existing model at {config.MODEL_SAVE_PATH}")
        print("Loading pre-trained model...")
        return load_trained_model(device)
    
    # Load preprocessed data
    print("[1/4] Loading preprocessed data...")
    train_loader, val_loader, test_loader, tokenizer, metadata = load_preprocessed_data()
    
    vocab_size = tokenizer.vocab_size
    print(f"  ✓ Vocabulary size: {vocab_size}")
    print(f"  ✓ Train batches: {len(train_loader)}")
    print(f"  ✓ Validation batches: {len(val_loader)}")
    print(f"  ✓ Test batches: {len(test_loader)}")
    
    # Initialize model
    print("\n[2/4] Initializing LSTM Autoencoder...")
    model = LSTMAutoencoder(
        vocab_size=vocab_size,
        embedding_dim=config.EMBEDDING_DIM,
        hidden_dim=config.HIDDEN_DIM,
        num_layers=config.NUM_LAYERS,
        latent_dim=config.LATENT_DIM,
        dropout=config.DROPOUT
    )
    model = model.to(device)
    
    # Count parameters
    num_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    print(f"  ✓ Model initialized")
    print(f"    - Parameters: {num_params:,}")
    print(f"    - Embedding dim: {config.EMBEDDING_DIM}")
    print(f"    - Hidden dim: {config.HIDDEN_DIM}")
    print(f"    - Latent dim: {config.LATENT_DIM}")
    print(f"    - Layers: {config.NUM_LAYERS}")
    
    # Train model
    print("\n[3/4] Training model...")
    history = train_model(
        model=model,
        train_loader=train_loader,
        val_loader=val_loader,
        num_epochs=num_epochs or config.NUM_EPOCHS,
        learning_rate=learning_rate or config.LEARNING_RATE,
        device=device,
        save_path=config.MODEL_SAVE_PATH,
        clip_grad_norm=config.CLIP_GRAD_NORM,
        early_stopping_patience=config.EARLY_STOPPING_PATIENCE
    )
    
    # Load best model
    print("\n[4/4] Loading best model and evaluating...")
    checkpoint = torch.load(config.MODEL_SAVE_PATH, map_location=device)
    model.load_state_dict(checkpoint['model_state_dict'])
    model.eval()
    
    # Evaluate on test set
    test_metrics, anomaly_scores, anomaly_labels = evaluate_anomaly_detection(
        model=model,
        test_loader=test_loader,
        device=device,
        threshold_percentile=config.ANOMALY_THRESHOLD_PERCENTILE
    )
    
    # Save evaluation results
    results_path = os.path.join(os.path.dirname(config.MODEL_SAVE_PATH), "evaluation_results.json")
    os.makedirs(os.path.dirname(config.MODEL_SAVE_PATH), exist_ok=True)
    with open(results_path, 'w') as f:
        json.dump(test_metrics, f, indent=2)
    print(f"  ✓ Saved evaluation results to {results_path}")
    
    # Display results
    print("\n" + "=" * 70)
    print("Anomaly Detection Evaluation")
    print("=" * 70)
    print(f"  • Test samples: {test_metrics['num_samples']:,}")
    print(f"  • Detected anomalies: {test_metrics['num_anomalies']:,} ({test_metrics['anomaly_rate']:.2f}%)")
    print(f"  • Normal sequences: {test_metrics['num_normal']:,}")
    print(f"  • Threshold: {test_metrics['threshold']:.4f} ({test_metrics['threshold_percentile']}th percentile)")
    print(f"\n  Anomaly Score Statistics:")
    print(f"    - Mean: {test_metrics['mean_score']:.4f}")
    print(f"    - Median: {test_metrics['median_score']:.4f}")
    print(f"    - Std: {test_metrics['std_score']:.4f}")
    print(f"    - Range: [{test_metrics['min_score']:.4f}, {test_metrics['max_score']:.4f}]")
    print("=" * 70)
    print("✓ Step 4 completed successfully!")
    print("=" * 70)
    
    return model, history, test_metrics


def load_trained_model(device: torch.device):
    """
    Load a pre-trained model.
    
    Args:
        device: Device to load model on
    
    Returns:
        Tuple of (model, history, test_metrics)
    """
    from steps.step3_preprocess import load_preprocessed_data
    
    # Load preprocessed data
    train_loader, val_loader, test_loader, tokenizer, metadata = load_preprocessed_data()
    
    # Initialize model
    model = LSTMAutoencoder(
        vocab_size=tokenizer.vocab_size,
        embedding_dim=config.EMBEDDING_DIM,
        hidden_dim=config.HIDDEN_DIM,
        num_layers=config.NUM_LAYERS,
        latent_dim=config.LATENT_DIM,
        dropout=config.DROPOUT
    )
    
    # Load weights
    checkpoint = torch.load(config.MODEL_SAVE_PATH, map_location=device)
    model.load_state_dict(checkpoint['model_state_dict'])
    model = model.to(device)
    model.eval()
    
    print(f"  ✓ Loaded model from epoch {checkpoint['epoch']}")
    print(f"  ✓ Best validation loss: {checkpoint['val_loss']:.4f}")
    
    # Evaluate
    print("\n[4/4] Evaluating model on test set...")
    from src.anomaly_detector import evaluate_anomaly_detection
    test_metrics, anomaly_scores, anomaly_labels = evaluate_anomaly_detection(
        model=model,
        test_loader=test_loader,
        device=device,
        threshold_percentile=config.ANOMALY_THRESHOLD_PERCENTILE
    )
    
    # Save evaluation results
    results_path = os.path.join(os.path.dirname(config.MODEL_SAVE_PATH), "evaluation_results.json")
    os.makedirs(os.path.dirname(config.MODEL_SAVE_PATH), exist_ok=True)
    with open(results_path, 'w') as f:
        json.dump(test_metrics, f, indent=2)
    print(f"  ✓ Saved evaluation results to {results_path}")
    
    # Display results
    print("\n" + "=" * 70)
    print("Anomaly Detection Evaluation")
    print("=" * 70)
    print(f"  • Test samples: {test_metrics['num_samples']:,}")
    print(f"  • Detected anomalies: {test_metrics['num_anomalies']:,} ({test_metrics['anomaly_rate']:.2f}%)")
    print(f"  • Normal sequences: {test_metrics['num_normal']:,}")
    print(f"  • Threshold: {test_metrics['threshold']:.4f} ({test_metrics['threshold_percentile']}th percentile)")
    print(f"\n  Anomaly Score Statistics:")
    print(f"    - Mean: {test_metrics['mean_score']:.4f}")
    print(f"    - Median: {test_metrics['median_score']:.4f}")
    print(f"    - Std: {test_metrics['std_score']:.4f}")
    print(f"    - Range: [{test_metrics['min_score']:.4f}, {test_metrics['max_score']:.4f}]")
    print("=" * 70)
    print("✓ Model evaluation completed!")
    print("=" * 70)
    
    return model, checkpoint.get('history', {}), test_metrics


def run_step4(force_retrain=False, num_epochs=None, learning_rate=None, device=None):
    """
    Execute Step 4: Train LSTM Autoencoder.
    
    Args:
        force_retrain: If True, retrain even if model exists
        num_epochs: Number of training epochs
        learning_rate: Learning rate
        device: Device to use
    
    Returns:
        Tuple of (model, history, test_metrics)
    """
    return train_autoencoder(
        force_retrain=force_retrain,
        num_epochs=num_epochs,
        learning_rate=learning_rate,
        device=device
    )

