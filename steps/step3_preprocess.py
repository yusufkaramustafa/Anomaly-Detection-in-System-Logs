"""
Step 3: Preprocess event sequences for model training.

This step:
1. Loads event sequences from Step 2
2. Tokenizes EventIds to integers
3. Splits data into train/val/test sets
4. Pads/truncates sequences to fixed length
5. Creates PyTorch DataLoaders
6. Saves preprocessed data and tokenizer
"""

import os
import pickle
import numpy as np
from typing import Tuple

from src.spark_utils import create_spark_session, stop_spark_session
from src.sequence_loader import load_sequences_from_spark, filter_sequences_by_length
from src.preprocessor import (
    EventTokenizer,
    pad_sequences,
    split_data,
    create_dataloaders
)
import config


def preprocess_sequences(
    force_reprocess: bool = False,
    max_seq_length: int = None,
    min_seq_length: int = None,
    batch_size: int = None
) -> Tuple:
    """
    Preprocess event sequences for model training.
    
    Args:
        force_reprocess: If True, reprocess even if preprocessed data exists
        max_seq_length: Maximum sequence length (defaults to config)
        min_seq_length: Minimum sequence length (defaults to config)
        batch_size: Batch size for DataLoaders (defaults to config)
    
    Returns:
        Tuple of (train_loader, val_loader, test_loader, tokenizer, metadata)
    """
    max_seq_length = max_seq_length or config.MAX_SEQUENCE_LENGTH
    min_seq_length = min_seq_length or config.MIN_SEQUENCE_LENGTH
    batch_size = batch_size or config.BATCH_SIZE
    
    # Check if preprocessed data exists
    tokenizer_path = os.path.join(config.PREPROCESSED_DATA_PATH, "tokenizer.pkl")
    if not force_reprocess and os.path.exists(tokenizer_path):
        print("Found existing preprocessed data. Loading from disk...")
        return load_preprocessed_data()
    
    spark = None
    try:
        print("=" * 70)
        print("Step 3: Preprocess Event Sequences for Model Training")
        print("=" * 70)
        
        # Step 1: Load sequences from Spark
        print("\n[1/6] Loading sequences from Spark...")
        spark = create_spark_session()
        sequences, block_ids, timestamps, metadata = load_sequences_from_spark(spark)
        
        # Step 2: Filter sequences by length
        print("\n[2/6] Filtering sequences by length...")
        sequences, block_ids, timestamps = filter_sequences_by_length(
            sequences,
            block_ids,
            timestamps,
            min_length=min_seq_length,
            max_length=config.MAX_SEQUENCE_LENGTH_FILTER
        )
        
        # Step 3: Build vocabulary and tokenize
        print("\n[3/6] Building vocabulary and tokenizing sequences...")
        tokenizer = EventTokenizer()
        tokenizer.build_vocab(sequences, min_freq=config.MIN_EVENT_FREQ)
        
        encoded_sequences = tokenizer.encode_batch(sequences)
        print(f"  ✓ Encoded {len(encoded_sequences):,} sequences")
        
        # Step 4: Split data
        print("\n[4/6] Splitting data into train/val/test sets...")
        train_seq, val_seq, test_seq = split_data(
            encoded_sequences,
            train_ratio=config.TRAIN_RATIO,
            val_ratio=config.VAL_RATIO,
            test_ratio=config.TEST_RATIO,
            shuffle=True,
            random_seed=config.RANDOM_SEED
        )
        
        print(f"  ✓ Train: {len(train_seq):,} sequences")
        print(f"  ✓ Validation: {len(val_seq):,} sequences")
        print(f"  ✓ Test: {len(test_seq):,} sequences")
        
        # Step 5: Pad sequences
        print(f"\n[5/6] Padding/truncating sequences to length {max_seq_length}...")
        train_padded = pad_sequences(
            train_seq,
            max_length=max_seq_length,
            pad_token=tokenizer.pad_token,
            truncate=True
        )
        val_padded = pad_sequences(
            val_seq,
            max_length=max_seq_length,
            pad_token=tokenizer.pad_token,
            truncate=True
        )
        test_padded = pad_sequences(
            test_seq,
            max_length=max_seq_length,
            pad_token=tokenizer.pad_token,
            truncate=True
        )
        
        print(f"  ✓ Train shape: {train_padded.shape}")
        print(f"  ✓ Validation shape: {val_padded.shape}")
        print(f"  ✓ Test shape: {test_padded.shape}")
        
        # Step 6: Create DataLoaders
        print("\n[6/6] Creating PyTorch DataLoaders...")
        train_loader, val_loader, test_loader = create_dataloaders(
            train_padded,
            val_padded,
            test_padded,
            batch_size=batch_size,
            shuffle_train=True
        )
        
        print(f"  ✓ Train batches: {len(train_loader)}")
        print(f"  ✓ Validation batches: {len(val_loader)}")
        print(f"  ✓ Test batches: {len(test_loader)}")
        
        # Save preprocessed data
        print("\n💾 Saving preprocessed data...")
        save_preprocessed_data(
            tokenizer,
            train_padded,
            val_padded,
            test_padded,
            metadata
        )
        
        # Display summary
        print("\n" + "=" * 70)
        print("Preprocessing Summary")
        print("=" * 70)
        print(f"  • Vocabulary size: {tokenizer.vocab_size}")
        print(f"  • Sequence length: {max_seq_length}")
        print(f"  • Train samples: {len(train_padded):,}")
        print(f"  • Validation samples: {len(val_padded):,}")
        print(f"  • Test samples: {len(test_padded):,}")
        print(f"  • Batch size: {batch_size}")
        print("=" * 70)
        print("✓ Step 3 completed successfully!")
        print("   Ready for model training")
        
        return train_loader, val_loader, test_loader, tokenizer, metadata
        
    except Exception as e:
        print(f"\n❌ Error in Step 3: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        if spark:
            stop_spark_session(spark)


def save_preprocessed_data(
    tokenizer: EventTokenizer,
    train_data: np.ndarray,
    val_data: np.ndarray,
    test_data: np.ndarray,
    metadata: dict
):
    """
    Save preprocessed data and tokenizer to disk.
    
    Args:
        tokenizer: EventTokenizer instance
        train_data: Training sequences array
        val_data: Validation sequences array
        test_data: Test sequences array
        metadata: Metadata dictionary
    """
    os.makedirs(config.PREPROCESSED_DATA_PATH, exist_ok=True)
    
    # Save tokenizer
    tokenizer_path = os.path.join(config.PREPROCESSED_DATA_PATH, "tokenizer.pkl")
    with open(tokenizer_path, "wb") as f:
        pickle.dump(tokenizer, f)
    print(f"  ✓ Saved tokenizer to {tokenizer_path}")
    
    # Save data arrays
    train_path = os.path.join(config.PREPROCESSED_DATA_PATH, "train.npy")
    val_path = os.path.join(config.PREPROCESSED_DATA_PATH, "val.npy")
    test_path = os.path.join(config.PREPROCESSED_DATA_PATH, "test.npy")
    
    np.save(train_path, train_data)
    np.save(val_path, val_data)
    np.save(test_path, test_data)
    
    print(f"  ✓ Saved data arrays to {config.PREPROCESSED_DATA_PATH}/")
    
    # Save metadata
    import json
    metadata_path = os.path.join(config.PREPROCESSED_DATA_PATH, "metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"  ✓ Saved metadata to {metadata_path}")


def load_preprocessed_data():
    """
    Load preprocessed data from disk.
    
    Returns:
        Tuple of (train_loader, val_loader, test_loader, tokenizer, metadata)
    """
    import json
    
    print(f"\n=== Loading preprocessed data from {config.PREPROCESSED_DATA_PATH} ===")
    
    # Load tokenizer
    tokenizer_path = os.path.join(config.PREPROCESSED_DATA_PATH, "tokenizer.pkl")
    with open(tokenizer_path, "rb") as f:
        tokenizer = pickle.load(f)
    print(f"  ✓ Loaded tokenizer (vocab_size={tokenizer.vocab_size})")
    
    # Load data arrays
    train_data = np.load(os.path.join(config.PREPROCESSED_DATA_PATH, "train.npy"))
    val_data = np.load(os.path.join(config.PREPROCESSED_DATA_PATH, "val.npy"))
    test_data = np.load(os.path.join(config.PREPROCESSED_DATA_PATH, "test.npy"))
    print(f"  ✓ Loaded data arrays")
    
    # Load metadata
    metadata_path = os.path.join(config.PREPROCESSED_DATA_PATH, "metadata.json")
    with open(metadata_path, "r") as f:
        metadata = json.load(f)
    
    # Create DataLoaders
    train_loader, val_loader, test_loader = create_dataloaders(
        train_data,
        val_data,
        test_data,
        batch_size=config.BATCH_SIZE,
        shuffle_train=False  # Don't shuffle when loading
    )
    
    print(f"  ✓ Created DataLoaders")
    
    return train_loader, val_loader, test_loader, tokenizer, metadata


def run_step3(force_reprocess=False, max_seq_length=None, min_seq_length=None, batch_size=None):
    """
    Execute Step 3: Preprocess sequences for training.
    
    Args:
        force_reprocess: If True, reprocess even if data exists
        max_seq_length: Maximum sequence length
        min_seq_length: Minimum sequence length
        batch_size: Batch size for DataLoaders
    
    Returns:
        Tuple of (train_loader, val_loader, test_loader, tokenizer, metadata)
    """
    return preprocess_sequences(
        force_reprocess=force_reprocess,
        max_seq_length=max_seq_length,
        min_seq_length=min_seq_length,
        batch_size=batch_size
    )

