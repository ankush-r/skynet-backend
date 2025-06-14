from fastapi import APIRouter, HTTPException
from app.utils.aws_operations import add_item_to_dynamodb
import uuid
from datetime import datetime

router = APIRouter()

@router.post("/sample")
async def add_sample_data():
    """
    Add a sample candidate record to DynamoDB
    """
    try:
        # Generate a unique ID for the sample candidate
        candidate_id = str(uuid.uuid4())
        
        # Create sample candidate data
        sample_data = {
            "id": candidate_id,
            "email": "sample.candidate@example.com",
            "absolute_scoring": 75,
            "culture_score": 80,
            "jd_score": 70,
            "verdict": False,
            "verdict_comment": "",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        # Add the sample data to DynamoDB
        success = add_item_to_dynamodb(sample_data)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to add sample data")
            
        return {
            "message": "Sample data added successfully",
            "candidate_id": candidate_id,
            "data": sample_data
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 