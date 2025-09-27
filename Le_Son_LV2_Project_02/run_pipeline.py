import argparse
from streaming_pipeline.pipeline import WarmupPipeline, StreamingPipeline

def main():
    parser = argparse.ArgumentParser(
        description="Run the real-time data streaming pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Examples:
            # Run warmup pipeline in test mode (default)
            python run_pipeline.py --warmup
            
            # Run streaming pipeline in test mode with foreachBatch (default)
            python run_pipeline.py
            
            # Run streaming pipeline in production mode with foreachBatch
            python run_pipeline.py --production
            
            # Run streaming pipeline in test mode with foreachPartition
            python run_pipeline.py --processing-mode foreachPartition
        """
    )
    
    # Pipeline mode selection
    mode_group = parser.add_mutually_exclusive_group(required=False)
    mode_group.add_argument(
        "--warmup", 
        action="store_true", 
        help="Run warmup pipeline to initialize database schema"
    )
    mode_group.add_argument(
        "--stream", 
        action="store_true", 
        help="Run streaming pipeline to process real-time data (default)"
    )
    
    # Test mode flag
    parser.add_argument(
        "--production", 
        action="store_true", 
        help="Run in production mode (default: test mode)"
    )
    
    # Processing mode for streaming pipeline
    parser.add_argument(
        "--processing-mode", 
        choices=["foreachPartition", "foreachBatch"], 
        default="foreachBatch",
        help="Choose processing mode for streaming pipeline (default: foreachBatch)"
    )
    
    args = parser.parse_args()
    
    # Test mode is default, production mode only when --production is specified
    test_mode = not args.production
    
    if args.warmup:
        print(f"🚀 Starting Warmup Pipeline (test_mode={test_mode})")
        warmup = WarmupPipeline(test_mode=test_mode)
        warmup.run()
        print("✅ Warmup Pipeline completed successfully")
    
    else:  # Default to streaming mode
        print(f"🚀 Starting Streaming Pipeline (test_mode={test_mode}, processing_mode={args.processing_mode})")
        stream = StreamingPipeline(test_mode=test_mode)
        stream.run(processing_mode=args.processing_mode)

if __name__ == "__main__":
    main()
