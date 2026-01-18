##serves ML predictions and analytics via REST API

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
import os
import sys

#adding project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.api.routers import analytics, segments, health

#init FastAPI app
app = FastAPI(
    title="Retail Intelligence API",
    description="Production-grade retail analytics and ML predictions",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

#CORS middleware for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  #in PROD, we'll specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#routers
app.include_router(health.router, prefix="/health", tags=["Health"])
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["Analytics"])
app.include_router(segments.router, prefix="/api/v1/segments", tags=["Segments"])


@app.get("/")
async def root():
    return {
        "message": "Retail Intelligence Platform API",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.now().isoformat(),
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/api/v1/info")
async def api_info():
    #API information and available endpoints
    return {
        "service": "Retail Intelligence Platform",
        "version": "1.0.0",
        "available_endpoints": {
            "analytics": {
                "customer_overview": "/api/v1/analytics/customers/overview",
                "product_overview": "/api/v1/analytics/products/overview",
                "revenue_trends": "/api/v1/analytics/revenue/trends"
            },
            "segments": {
                "list_segments": "/api/v1/segments/",
                "segment_details": "/api/v1/segments/{segment_name}",
                "customer_segment": "/api/v1/segments/customer/{customer_id}"
            },
            "health": {
                "health_check": "/health",
                "readiness": "/health/ready"
            }
        },
        "documentation": {
            "swagger_ui": "/docs",
            "redoc": "/redoc"
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )