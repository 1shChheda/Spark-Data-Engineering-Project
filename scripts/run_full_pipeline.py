#Complete Data Pipeline Orchestrator
#Runs: Bronze -> Silver -> Gold -> Features -> ML Models

import sys
import os
from datetime import datetime

#add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.spark_session import create_spark_session, stop_spark_session
from src.processing.bronze_layer import BronzeLayer
from src.processing.silver_layer import SilverLayer


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
        
    def run_bronze_layer(self):
        ##step 1: ingest raw data
        self.log_step("BRONZE LAYER: Raw Data Ingestion")
        
        bronze = BronzeLayer(self.spark)
        excel_file = "data/raw/Online_Retail.xlsx"
        
        if not os.path.exists(excel_file):
            raise FileNotFoundError(
                f"Excel file not found: {excel_file}\n"
                "Please place Online_Retail.xlsx in data/raw/ directory"
            )
        
        bronze.ingest_transactions(excel_file)
        self.log_step("BRONZE LAYER: Raw Data Ingestion", "DONE")
        
    def run_silver_layer(self):
        ##step 2: clean and validate data
        self.log_step("SILVER LAYER: Data Cleaning & Validation")
        
        silver = SilverLayer(self.spark)
        silver.clean_transactions()
        silver.create_customer_profiles()
        silver.create_product_catalog()
        
        self.log_step("SILVER LAYER: Data Cleaning & Validation", "DONE")
        
    def run_complete_pipeline(self, skip_bronze=False):
        #run complete end-to-end pipeline
        print("\n" + "="*80)
        print(" RETAIL INTELLIGENCE PLATFORM - COMPLETE PIPELINE")
        print("="*80)
        print(f"Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        try:
            ##step 1: Bronze layer
            if not skip_bronze:
                self.run_bronze_layer()
            else:
                print("\n⏭  Skipping Bronze layer (already exists)")
            
            ##step 2: Silver layer
            self.run_silver_layer()
            
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
    parser.add_argument(
        '--skip-bronze',
        action='store_true',
        help='Skip bronze layer ingestion (use existing data)'
    )
    parser.add_argument(
        '--step',
        choices=['bronze', 'silver'],
        help='Run only specific step'
    )
    
    args = parser.parse_args()
    
    #create Spark session
    spark = create_spark_session()
    
    try:
        orchestrator = PipelineOrchestrator(spark)
        
        if args.step:
            #run specific step
            step_map = {
                'bronze': orchestrator.run_bronze_layer,
                'silver': orchestrator.run_silver_layer
            }
            step_map[args.step]()
        else:
            #run complete pipeline
            orchestrator.run_complete_pipeline(skip_bronze=args.skip_bronze)
            
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        sys.exit(1)
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()