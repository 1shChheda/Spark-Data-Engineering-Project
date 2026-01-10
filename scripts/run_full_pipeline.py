#Complete Data Pipeline Orchestrator
#Runs: Bronze -> Silver -> Gold -> Features -> ML Models

import sys
import os
from datetime import datetime

#add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.spark_session import create_spark_session, stop_spark_session


class PipelineOrchestrator:
    #Orchestrates the complete data pipeline
    
    def __init__(self, spark):
        self.spark = spark
        self.start_time = datetime.now()
        
    def log_step(self, step_name, status="START"):
        #log pipeline step
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        symbol = "▶" if status == "START" else "✓" if status == "DONE" else "✗"
        print(f"\n{symbol} [{timestamp}] {step_name} - {status}")
        
    def run_complete_pipeline(self, skip_bronze=False):
        #run complete end-to-end pipeline
        print("\n" + "="*80)
        print(" RETAIL INTELLIGENCE PLATFORM - COMPLETE PIPELINE")
        print("="*80)
        print(f"Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S') }
        print("="*80)
        
        try:
            #TODO: implemention of steps
            pass
            
            #summary
            end_time = datetime.now()
            duration = (end_time - self.start_time).total_seconds()
            
            print("\n" + "="*80)
            print(" PIPELINE COMPLETED SUCCESSFULLY!")
            print("="*80)
            print(f"Start Time:    {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"End Time:      {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Duration:      {duration:.2f} seconds ({duration/60:.2f} minutes)")
            print("="*80)
            
        except Exception as e:
            print(f"\n✗ Pipeline failed: {str(e)}")
            import traceback
            traceback.print_exc()
            raise


def main():
    #Main execution
    import argparse
    
    parser = argparse.ArgumentParser(description='Run Retail Intelligence Pipeline')
    args = parser.parse_args()
    
    #create Spark session
    spark = create_spark_session()
    
    try:
        orchestrator = PipelineOrchestrator(spark)
        orchestrator.run_complete_pipeline(skip_bronze=args.skip_bronze)
            
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        sys.exit(1)
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()