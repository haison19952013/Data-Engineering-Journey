from pipeline import WarmupPipeline, StreamingPipeline

if __name__ == "__main__":
    # Warmup Pipeline
    if False:  # Change to True to run warmup
        warmup = WarmupPipeline(test_mode=True)
        warmup.run()
        exit()

    # # Stream Pipeline
    if True:  # Change to True to run stream
        stream = StreamingPipeline(test_mode=False)
        # Choose your processing mode: "foreachPartition" or "foreachBatch"
        processing_mode = "foreachBatch"
        stream.run(processing_mode=processing_mode)
