"""
Training utilities for LSTM Autoencoder.
"""

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
from tqdm import tqdm
import numpy as np
from typing import Dict, List, Tuple
import os


def train_epoch(
    model: nn.Module,
    train_loader: DataLoader,
    optimizer: optim.Optimizer,
    criterion: nn.Module,
    device: torch.device,
    clip_grad_norm: float = 1.0
) -> Dict[str, float]:
    """
    Train model for one epoch.
    
    Args:
        model: LSTM Autoencoder model
        train_loader: Training DataLoader
        optimizer: Optimizer
        criterion: Loss function
        device: Device to run on
        clip_grad_norm: Gradient clipping norm
    
    Returns:
        Dictionary with training metrics
    """
    model.train()
    total_loss = 0.0
    num_batches = 0
    
    progress_bar = tqdm(train_loader, desc="Training")
    
    for batch in progress_bar:
        # Move batch to device
        sequences = batch.to(device)  # (batch_size, seq_length)
        
        # Forward pass
        reconstructed_logits, latent = model(sequences)
        
        # Compute loss
        # Flatten for loss computation
        batch_size, seq_length, vocab_size = reconstructed_logits.shape
        logits_flat = reconstructed_logits.view(-1, vocab_size)
        targets_flat = sequences.view(-1)
        
        # Cross-entropy loss (ignore padding token 0)
        loss = criterion(logits_flat, targets_flat)
        
        # Backward pass
        optimizer.zero_grad()
        loss.backward()
        
        # Gradient clipping
        if clip_grad_norm > 0:
            torch.nn.utils.clip_grad_norm_(model.parameters(), clip_grad_norm)
        
        optimizer.step()
        
        # Accumulate loss
        total_loss += loss.item()
        num_batches += 1
        
        # Update progress bar
        progress_bar.set_postfix({'loss': f'{loss.item():.4f}'})
    
    avg_loss = total_loss / num_batches if num_batches > 0 else 0.0
    
    return {
        'loss': avg_loss,
        'num_batches': num_batches
    }


def validate(
    model: nn.Module,
    val_loader: DataLoader,
    criterion: nn.Module,
    device: torch.device
) -> Dict[str, float]:
    """
    Validate model on validation set.
    
    Args:
        model: LSTM Autoencoder model
        val_loader: Validation DataLoader
        criterion: Loss function
        device: Device to run on
    
    Returns:
        Dictionary with validation metrics
    """
    model.eval()
    total_loss = 0.0
    num_batches = 0
    
    with torch.no_grad():
        progress_bar = tqdm(val_loader, desc="Validation")
        
        for batch in progress_bar:
            sequences = batch.to(device)
            
            # Forward pass
            reconstructed_logits, latent = model(sequences)
            
            # Compute loss
            batch_size, seq_length, vocab_size = reconstructed_logits.shape
            logits_flat = reconstructed_logits.view(-1, vocab_size)
            targets_flat = sequences.view(-1)
            
            loss = criterion(logits_flat, targets_flat)
            
            total_loss += loss.item()
            num_batches += 1
            
            progress_bar.set_postfix({'loss': f'{loss.item():.4f}'})
    
    avg_loss = total_loss / num_batches if num_batches > 0 else 0.0
    
    return {
        'loss': avg_loss,
        'num_batches': num_batches
    }


def train_model(
    model: nn.Module,
    train_loader: DataLoader,
    val_loader: DataLoader,
    num_epochs: int,
    learning_rate: float,
    device: torch.device,
    save_path: str = None,
    clip_grad_norm: float = 1.0,
    early_stopping_patience: int = 5
) -> Dict[str, List[float]]:
    """
    Train LSTM Autoencoder model.
    
    Args:
        model: LSTM Autoencoder model
        train_loader: Training DataLoader
        val_loader: Validation DataLoader
        num_epochs: Number of training epochs
        learning_rate: Learning rate
        device: Device to run on
        save_path: Path to save best model
        clip_grad_norm: Gradient clipping norm
        early_stopping_patience: Early stopping patience
    
    Returns:
        Dictionary with training history
    """
    # Setup optimizer and loss
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)
    criterion = nn.CrossEntropyLoss(ignore_index=0)  # Ignore padding token
    
    # Training history
    history = {
        'train_loss': [],
        'val_loss': []
    }
    
    best_val_loss = float('inf')
    patience_counter = 0
    
    print(f"\n{'='*70}")
    print(f"Training LSTM Autoencoder")
    print(f"{'='*70}")
    print(f"Device: {device}")
    print(f"Epochs: {num_epochs}")
    print(f"Learning rate: {learning_rate}")
    print(f"{'='*70}\n")
    
    for epoch in range(1, num_epochs + 1):
        print(f"Epoch {epoch}/{num_epochs}")
        print("-" * 70)
        
        # Train
        train_metrics = train_epoch(
            model, train_loader, optimizer, criterion, device, clip_grad_norm
        )
        
        # Validate
        val_metrics = validate(model, val_loader, criterion, device)
        
        # Record history
        history['train_loss'].append(train_metrics['loss'])
        history['val_loss'].append(val_metrics['loss'])
        
        print(f"\nTrain Loss: {train_metrics['loss']:.4f}")
        print(f"Val Loss: {val_metrics['loss']:.4f}")
        
        # Save best model
        if val_metrics['loss'] < best_val_loss:
            best_val_loss = val_metrics['loss']
            patience_counter = 0
            
            if save_path:
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                torch.save({
                    'epoch': epoch,
                    'model_state_dict': model.state_dict(),
                    'optimizer_state_dict': optimizer.state_dict(),
                    'val_loss': val_metrics['loss'],
                    'history': history
                }, save_path)
                print(f"✓ Saved best model (val_loss: {best_val_loss:.4f})")
        else:
            patience_counter += 1
            print(f"  (No improvement, patience: {patience_counter}/{early_stopping_patience})")
        
        # Early stopping
        if patience_counter >= early_stopping_patience:
            print(f"\nEarly stopping triggered after {epoch} epochs")
            break
        
        print()
    
    print(f"{'='*70}")
    print(f"Training completed!")
    print(f"Best validation loss: {best_val_loss:.4f}")
    print(f"{'='*70}")
    
    return history

