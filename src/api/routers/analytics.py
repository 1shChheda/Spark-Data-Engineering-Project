from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg
import os

router = APIRouter()


class CustomerOverview(BaseModel):
    total_customers: int
    active_customers: int
    total_revenue: float
    avg_customer_value: float


class ProductOverview(BaseModel):
    total_products: int
    total_transactions: int
    avg_product_price: float


class SegmentSummary(BaseModel):
    segment_name: str
    customer_count: int
    total_revenue: float
    avg_frequency: float


def get_spark():
    from src.spark_session import create_spark_session
    return create_spark_session()


@router.get("/customers/overview", response_model=CustomerOverview)
async def get_customer_overview():
    #get overall customer analytics
    
    try:
        spark = get_spark()
        
        #read customer 360
        df = spark.read.format("delta").load("data/gold/customer360")
        
        #calculate metrics
        total_customers = df.count()
        active_customers = df.filter(col("recency_days") <= 90).count()
        total_revenue = df.agg(spark_sum("monetary")).collect()[0][0] or 0
        avg_customer_value = df.agg(avg("monetary")).collect()[0][0] or 0
        
        return CustomerOverview(
            total_customers=total_customers,
            active_customers=active_customers,
            total_revenue=round(total_revenue, 2),
            avg_customer_value=round(avg_customer_value, 2)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching customer overview: {str(e)}")


@router.get("/products/overview", response_model=ProductOverview)
async def get_product_overview():
    #get overall product analytics
    
    try:
        spark = get_spark()
        
        #read product metrics
        df = spark.read.format("delta").load("data/gold/product_metrics")
        
        total_products = df.count()
        total_transactions = df.agg(spark_sum("times_purchased")).collect()[0][0] or 0
        avg_price = df.agg(avg("avg_price")).collect()[0][0] or 0
        
        return ProductOverview(
            total_products=total_products,
            total_transactions=int(total_transactions),
            avg_product_price=round(avg_price, 2)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching product overview: {str(e)}")


@router.get("/segments/summary", response_model=List[SegmentSummary])
async def get_segments_summary():
    #get summary of all customer segments
    
    try:
        spark = get_spark()
        
        #read segments
        df = spark.read.format("delta").load("data/gold/customer_segments")
        
        #aggregate by segment
        segment_stats = df.groupBy("segment_name").agg(
            count("*").alias("customer_count"),
            spark_sum("monetary").alias("total_revenue"),
            avg("frequency").alias("avg_frequency")
        ).collect()
        
        results = []
        for row in segment_stats:
            results.append(SegmentSummary(
                segment_name=row["segment_name"],
                customer_count=row["customer_count"],
                total_revenue=round(row["total_revenue"] or 0, 2),
                avg_frequency=round(row["avg_frequency"] or 0, 2)
            ))
        
        return sorted(results, key=lambda x: x.total_revenue, reverse=True)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching segment summary: {str(e)}")
