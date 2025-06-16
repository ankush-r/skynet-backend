from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import os
from app.utils.aws_operations import get_candidates_by_score_range, update_candidate_verdict, get_all_candidates_by_job_id, get_candidate, S3_BUCKET_NAME, aws_region
from decimal import Decimal

router = APIRouter()

class CustomCriteriaScore(BaseModel):
    name: str
    score: int
    justification: str

class CandidateResponse(BaseModel):
    job_id: Optional[str] = None
    candidate_id: Optional[str] = None
    name: Optional[str] = None
    email: Optional[str] = None
    jd_score: Optional[float] = None
    jd_analysis_url: Optional[str] = None
    status: Optional[str] = None  # accepted/rejected/consideration
    verdict_comment: Optional[str] = None
    cultural_fit_score: Optional[float] = None
    cultural_analysis_url: Optional[str] = None
    uniqueness_score: Optional[float] = None
    custom_criteria_score: Optional[float] = None
    cultural_fit_justification: Optional[str] = None
    uniqueness_justification: Optional[str] = None
    absolute_score: Optional[float] = None
    resume_key: Optional[str] = None

class CandidateListResponse(BaseModel):
    candidate_id: str
    job_id: str
    s3_resume_key: Optional[str] = None
    name: str
    email: str
    s3_parsed_key: Optional[str] = None
    ingested_at: Optional[str] = None
    jd_score: Optional[float] = None
    cultural_fit_score: Optional[float] = None
    uniqueness_score: Optional[float] = None
    absolute_score: Optional[float] = None
    custom_criteria_scores: Optional[List[CustomCriteriaScore]] = None
    resume_key: Optional[str] = None

class VerdictRequest(BaseModel):
    job_id: str
    candidate_id: str
    verdict_comment: str

@router.get("/candidates/range", response_model=List[CandidateResponse])
async def get_candidates_in_range(min_score: Optional[int] = 45, max_score: Optional[int] = 55):
    """
    Get all candidates with absolute scores between min_score and max_score and status IN_CONSIDERATION
    """
    try:
        expression_values = {
            ':min_score': Decimal(str(min_score)),
            ':max_score': Decimal(str(max_score)),
            ':status': 'IN_CONSIDERATION'
        }
        candidates = get_candidates_by_score_range(min_score, max_score, status='IN_CONSIDERATION')
        if candidates is None:
            raise HTTPException(status_code=500, detail="Error fetching candidates")
        return candidates
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch candidates: {str(e)}")

@router.get("/candidates/getAllCandidates", response_model=List[CandidateListResponse])
async def get_all_candidates(job_id: str = "TL001"):
    """
    Get all candidates for a specific job_id
    """
    try:
        # Check if AWS credentials are configured
        missing_vars = []
        for var in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION', 'DYNAMODB_TABLE_NAME']:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "AWS configuration error",
                    "message": "Missing required AWS environment variables",
                    "missing_variables": missing_vars
                }
            )

        candidates = get_all_candidates_by_job_id(job_id)
        if candidates is None:
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "DynamoDB error",
                    "message": "Failed to fetch candidates from DynamoDB. Please check AWS configuration."
                }
            )
        return candidates
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal server error",
                "message": str(e)
            }
        )

@router.post("/candidates/reject")
async def reject_candidate(request: VerdictRequest):
    """
    Reject a candidate by updating their status to REJECTED
    """
    try:
        success, error_message = update_candidate_verdict(request.job_id, request.candidate_id, "REJECTED", request.verdict_comment)
        if not success:
            raise HTTPException(
                status_code=404 if "not found" in error_message.lower() else 500,
                detail=error_message
            )
        return {"message": "Candidate rejected successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to reject candidate: {str(e)}")

@router.post("/candidates/accept")
async def accept_candidate(request: VerdictRequest):
    """
    Accept a candidate by updating their status to ACCEPTED
    """
    try:
        success, error_message = update_candidate_verdict(request.job_id, request.candidate_id, "ACCEPTED", request.verdict_comment)
        if not success:
            raise HTTPException(
                status_code=404 if "not found" in error_message.lower() else 500,
                detail=error_message
            )
        return {"message": "Candidate accepted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to accept candidate: {str(e)}")

@router.get("/candidates/debug_get_candidate")
def debug_get_candidate():
    job_id = "TL001"
    candidate_id = "c123456"
    candidate = get_candidate(job_id, candidate_id)
    return candidate 