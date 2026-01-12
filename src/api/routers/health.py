from fastapi import APIRouter, status
from pydantic import BaseModel
from datetime import datetime
import os

router = APIRouter()


class HealthResponse(BaseModel):
    status: str
    timestamp: str
    version: str
    data_available: bool


@router.get("", response_model=HealthResponse, status_code=status.HTTP_200_OK)
async def health_check():
    
    #check if data directories exist
    data_dirs = [
        "data/bronze/transactions",
        "data/silver/transactions_clean",
        "data/gold/customer_360"
    ]
    
    data_available = all(os.path.exists(d) for d in data_dirs)
    
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        version="1.0.0",
        data_available=data_available
    )


@router.get("/ready")
async def readiness_check():
    #readiness check for K8s/docker
    return {"status": "ready", "timestamp": datetime.now().isoformat()}