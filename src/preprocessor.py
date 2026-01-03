"""
Preprocessing utilities for preparing event sequences for model training.
Handles tokenization, encoding, padding, and data splitting.
"""

import numpy as np
from collections import Counter
from typing import List, Tuple, Dict
import torch
from torch.utils.data import Dataset, DataLoader


class EventTokenizer:
    """
    Tokenizer for converting EventIds (E1, E2, ...) to integer tokens.
    """
    
    def __init__(self):
        self.event_to_id: Dict[str, int] = {}
        self.id_to_event: Dict[int, str] = {}
        self.vocab_size: int = 0
        self.unknown_token: int = 0
        self.pad_token: int = 0
        
    def build_vocab(self, sequences: List[List[str]], min_freq: int = 1):
        """
        Build vocabulary from sequences.
        
        Args:
            sequences: List of event sequences (each is a list of EventIds)
            min_freq: Minimum frequency for an event to be included in vocab
        """
        # Count event frequencies
        event_counts = Counter()
        for seq in sequences:
            event_counts.update(seq)
        
        # Build vocabulary (filter by min_freq)
        # Reserve 0 for padding, 1 for unknown
        self.event_to_id = {"<PAD>": 0, "<UNK>": 1}
        self.id_to_event = {0: "<PAD>", 1: "<UNK>"}
        
        token_id = 2
        for event, count in event_counts.most_common():
            if count >= min_freq:
                self.event_to_id[event] = token_id
                self.id_to_event[token_id] = event
                token_id += 1
        
        self.vocab_size = len(self.event_to_id)
        self.pad_token = 0
        self.unknown_token = 1
        
        print(f"  ✓ Built vocabulary: {self.vocab_size} tokens")
        print(f"    - {len(event_counts)} unique events found")
        print(f"    - {token_id - 2} events included (freq >= {min_freq})")
    
    def encode(self, sequence: List[str]) -> List[int]:
        """
        Encode a sequence of EventIds to integer tokens.
        
        Args:
            sequence: List of EventIds
        
        Returns:
            List of integer tokens
        """
        return [
            self.event_to_id.get(event, self.unknown_token)
            for event in sequence
        ]
    
    def encode_batch(self, sequences: List[List[str]]) -> List[List[int]]:
        """
        Encode a batch of sequences.
        
        Args:
            sequences: List of sequences (each is a list of EventIds)
        
        Returns:
            List of encoded sequences (each is a list of integers)
        """
        return [self.encode(seq) for seq in sequences]
    
    def decode(self, tokens: List[int]) -> List[str]:
        """
        Decode integer tokens back to EventIds.
        
        Args:
            tokens: List of integer tokens
        
        Returns:
            List of EventIds
        """
        return [
            self.id_to_event.get(token, "<UNK>")
            for token in tokens
        ]


def pad_sequences(
    sequences: List[List[int]], 
    max_length: int, 
    pad_token: int = 0,
    truncate: bool = True
) -> np.ndarray:
    """
    Pad or truncate sequences to fixed length.
    
    Args:
        sequences: List of sequences (each is a list of integers)
        max_length: Target sequence length
        pad_token: Token to use for padding
        truncate: If True, truncate sequences longer than max_length
    
    Returns:
        Numpy array of shape (num_sequences, max_length)
    """
    padded = []
    
    for seq in sequences:
        if len(seq) > max_length:
            if truncate:
                seq = seq[:max_length]
            else:
                # Keep full sequence if truncate=False
                pass
        else:
            # Pad sequence
            seq = seq + [pad_token] * (max_length - len(seq))
        
        padded.append(seq)
    
    return np.array(padded, dtype=np.int64)


def split_data(
    sequences: List[List[int]],
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
    test_ratio: float = 0.15,
    shuffle: bool = True,
    random_seed: int = 42,
    block_ids: List[str] = None,
    timestamps: List[List[str]] = None,
    sequence_lengths: List[int] = None,
    unique_events: List[int] = None,
    time_spans: List[float] = None
) -> Tuple[
    List[List[int]], List[List[int]], List[List[int]],
    List[str], List[str], List[str],
    List[List[str]], List[List[str]], List[List[str]],
    List[int], List[int], List[int],
    List[int], List[int], List[int],
    List[float], List[float], List[float]
]:
    """
    Split sequences into train/validation/test sets (unsupervised learning).
    
    Args:
        sequences: List of sequences
        train_ratio: Proportion for training set
        val_ratio: Proportion for validation set
        test_ratio: Proportion for test set
        shuffle: Whether to shuffle before splitting
        random_seed: Random seed for reproducibility
    
    Returns:
        Tuple of (train_sequences, val_sequences, test_sequences,
                  train_block_ids, val_block_ids, test_block_ids,
                  train_timestamps, val_timestamps, test_timestamps)
    """
    assert abs(train_ratio + val_ratio + test_ratio - 1.0) < 1e-6, \
        "Ratios must sum to 1.0"
    
    if block_ids is not None and len(block_ids) != len(sequences):
        raise ValueError("block_ids must align with sequences")
    
    if timestamps is not None and len(timestamps) != len(sequences):
        raise ValueError("timestamps must align with sequences")
    if sequence_lengths is not None and len(sequence_lengths) != len(sequences):
        raise ValueError("sequence_lengths must align with sequences")
    if unique_events is not None and len(unique_events) != len(sequences):
        raise ValueError("unique_events must align with sequences")
    if time_spans is not None and len(time_spans) != len(sequences):
        raise ValueError("time_spans must align with sequences")
    
    if shuffle:
        np.random.seed(random_seed)
        indices = np.random.permutation(len(sequences))
        sequences = [sequences[i] for i in indices]
        if block_ids is not None:
            block_ids = [block_ids[i] for i in indices]
        if timestamps is not None:
            timestamps = [timestamps[i] for i in indices]
        if sequence_lengths is not None:
            sequence_lengths = [sequence_lengths[i] for i in indices]
        if unique_events is not None:
            unique_events = [unique_events[i] for i in indices]
        if time_spans is not None:
            time_spans = [time_spans[i] for i in indices]
    
    n = len(sequences)
    n_train = int(n * train_ratio)
    n_val = int(n * val_ratio)
    
    train_seq = sequences[:n_train]
    val_seq = sequences[n_train:n_train + n_val]
    test_seq = sequences[n_train + n_val:]
    
    if block_ids is not None:
        train_ids = block_ids[:n_train]
        val_ids = block_ids[n_train:n_train + n_val]
        test_ids = block_ids[n_train + n_val:]
    else:
        train_ids = val_ids = test_ids = None
    
    if timestamps is not None:
        train_ts = timestamps[:n_train]
        val_ts = timestamps[n_train:n_train + n_val]
        test_ts = timestamps[n_train + n_val:]
    else:
        train_ts = val_ts = test_ts = None

    if sequence_lengths is not None:
        train_len = sequence_lengths[:n_train]
        val_len = sequence_lengths[n_train:n_train + n_val]
        test_len = sequence_lengths[n_train + n_val:]
    else:
        train_len = val_len = test_len = None

    if unique_events is not None:
        train_unique = unique_events[:n_train]
        val_unique = unique_events[n_train:n_train + n_val]
        test_unique = unique_events[n_train + n_val:]
    else:
        train_unique = val_unique = test_unique = None

    if time_spans is not None:
        train_span = time_spans[:n_train]
        val_span = time_spans[n_train:n_train + n_val]
        test_span = time_spans[n_train + n_val:]
    else:
        train_span = val_span = test_span = None

    return (
        train_seq, val_seq, test_seq,
        train_ids, val_ids, test_ids,
        train_ts, val_ts, test_ts,
        train_len, val_len, test_len,
        train_unique, val_unique, test_unique,
        train_span, val_span, test_span
    )


class SequenceDataset(Dataset):
    """
    PyTorch Dataset for event sequences (unsupervised learning).
    Returns sequences only, no labels.
    """
    
    def __init__(self, sequences: np.ndarray):
        """
        Args:
            sequences: Numpy array of shape (num_samples, seq_length)
        """
        self.sequences = torch.LongTensor(sequences)
    
    def __len__(self):
        return len(self.sequences)
    
    def __getitem__(self, idx):
        return self.sequences[idx]


def create_dataloaders(
    train_sequences: np.ndarray,
    val_sequences: np.ndarray,
    test_sequences: np.ndarray,
    batch_size: int = 32,
    shuffle_train: bool = True
) -> Tuple[DataLoader, DataLoader, DataLoader]:
    """
    Create PyTorch DataLoaders for train/val/test sets (unsupervised learning).
    
    Args:
        train_sequences: Training sequences array
        val_sequences: Validation sequences array
        test_sequences: Test sequences array
        batch_size: Batch size for DataLoaders
        shuffle_train: Whether to shuffle training data
    
    Returns:
        Tuple of (train_loader, val_loader, test_loader)
    """
    train_dataset = SequenceDataset(train_sequences)
    val_dataset = SequenceDataset(val_sequences)
    test_dataset = SequenceDataset(test_sequences)
    
    train_loader = DataLoader(
        train_dataset,
        batch_size=batch_size,
        shuffle=shuffle_train,
        num_workers=0,  # Set to 0 to avoid multiprocessing issues
        pin_memory=True
    )
    
    val_loader = DataLoader(
        val_dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=0,
        pin_memory=True
    )
    
    test_loader = DataLoader(
        test_dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=0,
        pin_memory=True
    )
    
    return train_loader, val_loader, test_loader
