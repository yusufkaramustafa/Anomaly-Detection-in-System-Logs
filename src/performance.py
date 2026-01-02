"""
Performance tracking utilities for pipeline steps.

Tracks execution time, data sizes, memory usage, and CPU usage
for each step and substep of the pipeline.
"""

import os
import json
import time
import psutil
import threading
from contextlib import contextmanager
from typing import Dict, List, Optional, Any
from datetime import datetime


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string."""
    if seconds < 1:
        return f"{seconds * 1000:.2f} ms"
    elif seconds < 60:
        return f"{seconds:.2f} s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.2f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.2f}s"


def get_directory_size(path: str) -> int:
    """Get total size of directory in bytes."""
    total = 0
    try:
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.exists(filepath):
                    total += os.path.getsize(filepath)
    except Exception:
        pass
    return total


def format_bytes(bytes_size: int) -> str:
    """Format bytes to human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"


class ResourceMonitor:
    """Monitor CPU and memory usage in a separate thread."""
    
    def __init__(self, interval: float = 0.1):
        """
        Initialize resource monitor.
        
        Args:
            interval: Sampling interval in seconds
        """
        self.interval = interval
        self.monitoring = False
        self.thread = None
        self.cpu_samples = []
        self.memory_samples = []
        self.process = psutil.Process(os.getpid())
    
    def _monitor(self):
        """Monitor loop running in separate thread."""
        while self.monitoring:
            try:
                cpu_percent = self.process.cpu_percent(interval=None)
                memory_info = self.process.memory_info()
                memory_mb = memory_info.rss / (1024 * 1024)
                
                self.cpu_samples.append(cpu_percent)
                self.memory_samples.append(memory_mb)
                
                time.sleep(self.interval)
            except Exception:
                break
    
    def start(self):
        """Start monitoring."""
        self.monitoring = True
        self.cpu_samples = []
        self.memory_samples = []
        self.thread = threading.Thread(target=self._monitor, daemon=True)
        self.thread.start()
    
    def stop(self) -> Dict[str, float]:
        """
        Stop monitoring and return statistics.
        
        Returns:
            Dictionary with CPU and memory statistics
        """
        self.monitoring = False
        if self.thread:
            self.thread.join(timeout=1.0)
        
        if not self.cpu_samples:
            return {
                'cpu_avg': 0.0,
                'cpu_max': 0.0,
                'memory_avg_mb': 0.0,
                'memory_max_mb': 0.0,
                'memory_min_mb': 0.0
            }
        
        return {
            'cpu_avg': sum(self.cpu_samples) / len(self.cpu_samples),
            'cpu_max': max(self.cpu_samples),
            'memory_avg_mb': sum(self.memory_samples) / len(self.memory_samples),
            'memory_max_mb': max(self.memory_samples),
            'memory_min_mb': min(self.memory_samples),
            'num_samples': len(self.cpu_samples)
        }


class PerformanceTracker:
    """Track performance metrics for pipeline steps."""
    
    def __init__(self, step_name: str, output_dir: str = "results/performance"):
        """
        Initialize performance tracker.
        
        Args:
            step_name: Name of the step being tracked (e.g., "step1", "step2")
            output_dir: Directory to save performance reports
        """
        self.step_name = step_name
        self.output_dir = output_dir
        self.start_time = None
        self.end_time = None
        self.substeps = []
        self.data_sizes = {}
        self.file_sizes = {}
        self.resource_monitor = ResourceMonitor()
        self.resource_stats = None
        
        os.makedirs(output_dir, exist_ok=True)
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
        if exc_type is None:
            self.save()
    
    def start(self):
        """Start tracking."""
        self.start_time = time.time()
        self.resource_monitor.start()
    
    def stop(self):
        """Stop tracking."""
        self.end_time = time.time()
        self.resource_stats = self.resource_monitor.stop()
    
    def record_substep(self, substep_name: str, duration: float, **metadata):
        """
        Record a substep with its duration and optional metadata.
        
        Args:
            substep_name: Name of the substep
            duration: Duration in seconds
            **metadata: Additional metadata to store
        """
        self.substeps.append({
            'name': substep_name,
            'duration_seconds': duration,
            'duration_formatted': format_duration(duration),
            **metadata
        })
    
    def record_data_size(self, name: str, row_count: int = None, file_size_bytes: int = None):
        """
        Record data size information.
        
        Args:
            name: Name/description of the data
            row_count: Number of rows (if applicable)
            file_size_bytes: File size in bytes (if applicable)
        """
        if name not in self.data_sizes:
            self.data_sizes[name] = {}
        
        if row_count is not None:
            self.data_sizes[name]['row_count'] = row_count
            self.data_sizes[name]['row_count_formatted'] = f"{row_count:,}"
        
        if file_size_bytes is not None:
            self.data_sizes[name]['file_size_bytes'] = file_size_bytes
            self.data_sizes[name]['file_size_formatted'] = format_bytes(file_size_bytes)
    
    def record_file_size(self, name: str, path: str):
        """
        Record file or directory size.
        
        Args:
            name: Name/description of the file/directory
            path: Path to file or directory
        """
        if not os.path.exists(path):
            return
        
        if os.path.isfile(path):
            size = os.path.getsize(path)
        else:
            size = get_directory_size(path)
        
        self.file_sizes[name] = {
            'path': path,
            'size_bytes': size,
            'size_formatted': format_bytes(size)
        }
    
    @contextmanager
    def track_substep(self, substep_name: str, **metadata):
        """
        Context manager for tracking a substep.
        
        Usage:
            with tracker.track_substep("load_data"):
                # code to track
                pass
        """
        start = time.time()
        try:
            yield
        finally:
            duration = time.time() - start
            self.record_substep(substep_name, duration, **metadata)
    
    def get_total_duration(self) -> float:
        """Get total duration in seconds."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        elif self.start_time:
            return time.time() - self.start_time
        return 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert tracker to dictionary for JSON serialization."""
        return {
            'step_name': self.step_name,
            'timestamp': datetime.now().isoformat(),
            'total_duration_seconds': self.get_total_duration(),
            'total_duration_formatted': format_duration(self.get_total_duration()),
            'substeps': self.substeps,
            'data_sizes': self.data_sizes,
            'file_sizes': self.file_sizes,
            'resource_usage': self.resource_stats or {},
            'summary': {
                'num_substeps': len(self.substeps),
                'total_substep_duration': sum(s['duration_seconds'] for s in self.substeps),
                'num_data_sizes': len(self.data_sizes),
                'num_file_sizes': len(self.file_sizes)
            }
        }
    
    def save(self, filename: Optional[str] = None):
        """
        Save performance report to JSON file.
        
        Args:
            filename: Optional custom filename (defaults to {step_name}_performance.json)
        """
        if filename is None:
            filename = f"{self.step_name}_performance.json"
        
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
        
        return filepath
    
    def print_summary(self):
        """Print a formatted summary of performance metrics."""
        print("\n" + "=" * 70)
        print(f"Performance Summary: {self.step_name.upper()}")
        print("=" * 70)
        
        total_duration = self.get_total_duration()
        print(f"\n⏱️  Total Duration: {format_duration(total_duration)}")
        
        if self.resource_stats:
            print(f"\n💻 Resource Usage:")
            print(f"  • CPU: Avg {self.resource_stats['cpu_avg']:.1f}%, Max {self.resource_stats['cpu_max']:.1f}%")
            print(f"  • Memory: Avg {self.resource_stats['memory_avg_mb']:.1f} MB, "
                  f"Max {self.resource_stats['memory_max_mb']:.1f} MB, "
                  f"Min {self.resource_stats['memory_min_mb']:.1f} MB")
        
        if self.substeps:
            print(f"\n📋 Substeps ({len(self.substeps)}):")
            for substep in self.substeps:
                pct = (substep['duration_seconds'] / total_duration * 100) if total_duration > 0 else 0
                print(f"  • {substep['name']}: {substep['duration_formatted']} ({pct:.1f}%)")
        
        if self.data_sizes:
            print(f"\n📊 Data Sizes:")
            for name, info in self.data_sizes.items():
                parts = []
                if 'row_count_formatted' in info:
                    parts.append(f"{info['row_count_formatted']} rows")
                if 'file_size_formatted' in info:
                    parts.append(f"{info['file_size_formatted']}")
                if parts:
                    print(f"  • {name}: {', '.join(parts)}")
        
        if self.file_sizes:
            print(f"\n💾 File Sizes:")
            for name, info in self.file_sizes.items():
                print(f"  • {name}: {info['size_formatted']} ({info['path']})")
        
        print("=" * 70)


def load_performance_report(step_name: str, output_dir: str = "results/performance") -> Optional[Dict]:
    """
    Load a performance report from disk.
    
    Args:
        step_name: Name of the step
        output_dir: Directory containing performance reports
    
    Returns:
        Dictionary with performance data or None if not found
    """
    filepath = os.path.join(output_dir, f"{step_name}_performance.json")
    if not os.path.exists(filepath):
        return None
    
    with open(filepath, 'r') as f:
        return json.load(f)


def print_all_performance_reports(output_dir: str = "results/performance"):
    """
    Print summaries of all available performance reports.
    
    Args:
        output_dir: Directory containing performance reports
    """
    if not os.path.exists(output_dir):
        print(f"No performance reports found in {output_dir}")
        return
    
    reports = []
    for filename in os.listdir(output_dir):
        if filename.endswith('_performance.json'):
            step_name = filename.replace('_performance.json', '')
            report = load_performance_report(step_name, output_dir)
            if report:
                reports.append((step_name, report))
    
    if not reports:
        print(f"No performance reports found in {output_dir}")
        return
    
    reports.sort(key=lambda x: x[0])
    
    print("\n" + "=" * 70)
    print("All Performance Reports")
    print("=" * 70)
    
    for step_name, report in reports:
        print(f"\n{step_name.upper()}:")
        print(f"  Duration: {report['total_duration_formatted']}")
        if report.get('resource_usage'):
            ru = report['resource_usage']
            print(f"  CPU: Avg {ru.get('cpu_avg', 0):.1f}%, Max {ru.get('cpu_max', 0):.1f}%")
            print(f"  Memory: Avg {ru.get('memory_avg_mb', 0):.1f} MB, Max {ru.get('memory_max_mb', 0):.1f} MB")
        if report.get('data_sizes'):
            print(f"  Data Points: {len(report['data_sizes'])}")
    
    print("\n" + "=" * 70)

