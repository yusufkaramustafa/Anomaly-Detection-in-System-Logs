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

from steps.step1_parse_logs import run_step1
from steps.step2_build_sequences import run_step2


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
    print("  Step 3: Train unsupervised model (LSTM/Transformer) - Coming soon")
    print("  Step 4: Generate anomaly scores - Coming soon")
    print("  Step 5: Evaluate and visualize results - Coming soon")
    print()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="HDFS Anomaly Detection Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run.py step1                    # Run step 1
  python run.py step1 --force            # Force re-parse
  python run.py step2                    # Run step 2
  python run.py step2 --min-length 2     # Filter sequences with min length 2
  python run.py all                      # Run all available steps
        """
    )
    
    parser.add_argument(
        "step",
        choices=["step1", "step2", "all", "info"],
        help="Step to execute (step1, step2, all, or info)"
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
    
    args = parser.parse_args()
    
    # Print banner
    print_banner()
    
    # Handle info command
    if args.step == "info":
        print_step_info()
        return
    
    # Execute steps
    try:
        if args.step == "step1" or args.step == "all":
            print("\n🚀 Starting Step 1...\n")
            run_step1(
                force_reparse=args.force_parse,
                log_path=args.log_path,
                template_path=args.template_path
            )
            print()
        
        if args.step == "step2" or args.step == "all":
            print("\n🚀 Starting Step 2...\n")
            run_step2(
                force_rebuild=args.force_rebuild,
                min_sequence_length=args.min_length
            )
            print()
        
        if args.step == "all":
            print("\n" + "=" * 70)
            print("✅ All steps completed successfully!")
            print("=" * 70)
            print("\nNext: Run Step 3 to train the unsupervised model")
            print()
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Pipeline failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

