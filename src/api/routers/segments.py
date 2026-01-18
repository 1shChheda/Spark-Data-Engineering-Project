from fastapi import APIRouter, HTTPException, Path
from pydantic import BaseModel
from typing import List, Optional
from pyspark.sql.functions import col
import os

router = APIRouter()


class CustomerSegment(BaseModel):
    customer_id: int
    segment_name: str
    cluster_id: int
    recency_days: int
    frequency: int
    monetary: float
    avg_transaction_value: float


class SegmentDetail(BaseModel):
    segment_name: str
    customer_count: int
    avg_recency: float
    avg_frequency: float
    avg_monetary: float
    total_revenue: float


@router.get("/", response_model=List[str])
async def list_segments():
    #get list of all available segments
    
    try:
        from src.spark_session import create_spark_session
        spark = create_spark_session()
        
        df = spark.read.format("delta").load("data/gold/customer_segments")
        segments = [row["segment_name"] for row in df.select("segment_name").distinct().collect()]
        
        return sorted(segments)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing segments: {str(e)}")


@router.get("/{segment_name}", response_model=SegmentDetail)
async def get_segment_details(
    segment_name: str = Path(..., description="Name of the segment")
):
    #get detailed information about a specific segment
    
    try:
        from src.spark_session import create_spark_session
        spark = create_spark_session()
        
        df = spark.read.format("delta").load("data/gold/customer_segments")
        
        #filter for segment
        segment_df = df.filter(col("segment_name") == segment_name)
        
        if segment_df.count() == 0:
            raise HTTPException(status_code=404, detail=f"Segment '{segment_name}' not found")
        
        #calculate statistics
        from pyspark.sql.functions import count, avg, sum as spark_sum
        
        stats = segment_df.agg(
            count("*").alias("customer_count"),
            avg("recency_days").alias("avg_recency"),
            avg("frequency").alias("avg_frequency"),
            avg("monetary").alias("avg_monetary"),
            spark_sum("monetary").alias("total_revenue")
        ).collect()[0]
        
        return SegmentDetail(
            segment_name=segment_name,
            customer_count=stats["customer_count"],
            avg_recency=round(stats["avg_recency"] or 0, 2),
            avg_frequency=round(stats["avg_frequency"] or 0, 2),
            avg_monetary=round(stats["avg_monetary"] or 0, 2),
            total_revenue=round(stats["total_revenue"] or 0, 2)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching segment details: {str(e)}")


@router.get("/customer/{customer_id}", response_model=CustomerSegment)
async def get_customer_segment(
    customer_id: int = Path(..., description="Customer ID")
):
    #get segment information for a specific customer
    
    try:
        from src.spark_session import create_spark_session
        spark = create_spark_session()
        
        df = spark.read.format("delta").load("data/gold/customer_segments")
        
        #filter for customer
        customer_df = df.filter(col("CustomerID") == customer_id)
        
        if customer_df.count() == 0:
            raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")
        
        row = customer_df.first()
        
        return CustomerSegment(
            customer_id=row["CustomerID"],
            segment_name=row["segment_name"],
            cluster_id=row["cluster_id"],
            recency_days=row["recency_days"],
            frequency=row["frequency"],
            monetary=round(row["monetary"], 2),
            avg_transaction_value=round(row["avg_transaction_value"], 2)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching customer segment: {str(e)}")
