#!/usr/bin/env python3
"""
HDFS Anomaly Detection Pipeline - CLI Entry Point

Run different steps of the pipeline using command-line arguments.
"""

import os
import sys
import argparse

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from steps.parse_logs import run_parse_logs
from steps.build_sequences import run_build_sequences
from steps.preprocess import run_preprocess
from steps.train_model import run_train_model
from steps.visualize import run_visualize
from src.performance import print_all_performance_reports, load_performance_report
from scripts.benchmark_scalability import run_benchmark
import config


def print_banner():
    """Print welcome banner."""
    banner = """
    ╔══════════════════════════════════════════════════════════════════╗
    ║     HDFS Anomaly Detection Pipeline - Unsupervised Learning      ║
    ╚══════════════════════════════════════════════════════════════════╝
    """
    print(banner)


def print_step_info():
    """Print information about available steps."""
    print("\n📋 Available Steps:")
    print("  Step 1: Parse raw HDFS logs and match to event templates")
    print("  Step 2: Build event sequences by block ID")
    print("  Step 3: Preprocess sequences for model training")
    print("  Step 4: Train LSTM Autoencoder for anomaly detection")
    print("  Step 5: Visualize anomaly detection results")
    print("  Benchmark: Compare Spark vs pandas sequence building performance")
    print()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="HDFS Anomaly Detection Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run.py step1                    # Run step 1
  python run.py step1 --force-parse      # Force re-parse
  python run.py step2                    # Run step 2
  python run.py step2 --min-length 2     # Filter sequences with min length 2
  python run.py step3                    # Preprocess sequences
  python run.py step3 --max-seq-length 50 # Custom sequence length
  python run.py step4                    # Train LSTM Autoencoder
  python run.py step4 --num-epochs 30    # Custom number of epochs
  python run.py step5                    # Visualize results
  python run.py step5 --top-k 100        # Analyze top 100 anomalies
  python run.py all                      # Run all available steps
        """
    )
    
    parser.add_argument(
        "step",
        choices=["step1", "step2", "step3", "step4", "step5", "all", "info", "performance", "benchmark"],
        help="Step to execute (step1-5, all, info, performance, or benchmark)"
    )
    
    # Step 1 arguments
    parser.add_argument(
        "--force-parse",
        action="store_true",
        help="Force re-parsing even if parsed data exists (Step 1)"
    )
    
    parser.add_argument(
        "--log-path",
        type=str,
        help="Path to raw HDFS log file (Step 1)"
    )
    
    parser.add_argument(
        "--template-path",
        type=str,
        help="Path to event templates CSV file (Step 1)"
    )
    
    # Step 2 arguments
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Force rebuild even if sequences exist (Step 2)"
    )
    
    parser.add_argument(
        "--min-length",
        type=int,
        default=1,
        help="Minimum sequence length to keep (Step 2, default: 1)"
    )
    
    # Step 3 arguments
    parser.add_argument(
        "--force-reprocess",
        action="store_true",
        help="Force reprocessing even if preprocessed data exists (Step 3)"
    )
    
    parser.add_argument(
        "--max-seq-length",
        type=int,
        help="Maximum sequence length for padding (Step 3)"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Batch size for DataLoaders (Step 3)"
    )
    
    # Step 4 arguments
    parser.add_argument(
        "--force-retrain",
        action="store_true",
        help="Force retraining even if model exists (Step 4)"
    )
    
    parser.add_argument(
        "--num-epochs",
        type=int,
        help="Number of training epochs (Step 4)"
    )
    
    parser.add_argument(
        "--learning-rate",
        type=float,
        help="Learning rate for training (Step 4)"
    )
    
    parser.add_argument(
        "--device",
        type=str,
        choices=["cuda", "mps", "cpu"],
        help="Device to use for training (Step 4)"
    )
    
    # Step 5 arguments
    parser.add_argument(
        "--force-recompute",
        action="store_true",
        help="Force recompute visualizations even if they exist (Step 5)"
    )
    
    parser.add_argument(
        "--top-k",
        type=int,
        default=50,
        help="Number of top anomalies to analyze in detail (Step 5, default: 50)"
    )

    # Benchmark arguments
    parser.add_argument(
        "--benchmark-mode",
        type=str,
        choices=["both", "spark", "pandas"],
        default="both",
        help="Benchmark mode (Spark vs pandas). Only used with step=benchmark."
    )
    
    args = parser.parse_args()
    
    # Print banner
    print_banner()
    
    # Handle info command
    if args.step == "info":
        print_step_info()
        return
    
    # Handle performance command
    if args.step == "performance":
        print_all_performance_reports(config.PERFORMANCE_OUTPUT_DIR)
        return
    
    # Handle benchmark command
    if args.step == "benchmark":
        run_spark = args.benchmark_mode in ["both", "spark"]
        run_pandas = args.benchmark_mode in ["both", "pandas"]
        print("\n🚀 Running scalability benchmark...\n")
        run_benchmark(run_spark=run_spark, run_pandas=run_pandas)
        return
    
    # Execute steps
    try:
        if args.step == "step1" or args.step == "all":
            print("\n🚀 Starting Step 1...\n")
            run_parse_logs(
                force_reparse=args.force_parse,
                log_path=args.log_path,
                template_path=args.template_path
            )
            print()
        
        if args.step == "step2" or args.step == "all":
            print("\n🚀 Starting Step 2...\n")
            run_build_sequences(
                force_rebuild=args.force_rebuild,
                min_sequence_length=args.min_length
            )
            print()
        
        if args.step == "step3" or args.step == "all":
            print("\n🚀 Starting Step 3...\n")
            run_preprocess(
                force_reprocess=args.force_reprocess,
                max_seq_length=args.max_seq_length,
                min_seq_length=args.min_length,
                batch_size=args.batch_size
            )
            print()
        
        if args.step == "step4" or args.step == "all":
            print("\n🚀 Starting Step 4...\n")
            run_train_model(
                force_retrain=args.force_retrain,
                num_epochs=args.num_epochs,
                learning_rate=args.learning_rate,
                device=args.device
            )
            print()
        
        if args.step == "step5" or args.step == "all":
            print("\n🚀 Starting Step 5...\n")
            run_visualize(
                force_recompute=args.force_recompute,
                top_k=args.top_k
            )
            print()
        
        if args.step == "all":
            print("\n" + "=" * 70)
            print("✅ All steps completed successfully!")
            print("=" * 70)
            print()
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Pipeline failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
