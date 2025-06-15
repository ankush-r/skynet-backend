from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import os
from app.utils.aws_operations import get_candidates_by_score_range, update_candidate_verdict, get_all_candidates_by_job_id, get_candidate, upload_to_s3, update_candidate_questions, s3_key_to_url, S3_BUCKET_NAME, aws_region
from app.utils.llm_operations import generate_interview_questions
from decimal import Decimal

router = APIRouter()

class CustomCriteriaScore(BaseModel):
    name: str
    score: int
    justification: str

class CandidateResponse(BaseModel):
    job_id: str
    candidate_id: str
    name: str
    email: str
    jd_score: float
    jd_analysis_url: str
    status: str  # accepted/rejected/consideration
    verdict_comment: str
    cultural_fit_score: float
    cultural_analysis_url: Optional[str] = None
    uniqueness_score: float
    custom_criteria_score: Optional[float] = None
    cultural_fit_justification: str
    uniqueness_justification: str
    absolute_score: float

class CandidateListResponse(BaseModel):
    candidate_id: str
    job_id: str
    s3_resume_key: Optional[str] = None
    name: str
    email: str
    s3_parsed_key: Optional[str] = None
    ingested_at: Optional[str] = None
    jd_score: Optional[int] = None
    cultural_fit_score: Optional[int] = None
    uniqueness_score: Optional[int] = None
    absolute_score: Optional[int] = None
    custom_criteria_scores: Optional[List[CustomCriteriaScore]] = None
    questions: Optional[str] = None  # S3 URL to questions.json

class VerdictRequest(BaseModel):
    job_id: str
    candidate_id: str
    verdict_comment: str

class QuestionsRequest(BaseModel):
    job_id: str
    candidate_id: str

class QuestionsResponse(BaseModel):
    s3_key: str
    questions: Dict[str, List[str]]

@router.get("/candidates/range", response_model=List[CandidateResponse])
async def get_candidates_in_range(min_score: Optional[int] = 45, max_score: Optional[int] = 55):
    """
    Get all candidates with absolute scores between min_score and max_score
    """
    try:
        expression_values = {
            ':min_score': Decimal(str(min_score)),
            ':max_score': Decimal(str(max_score))
        }
        candidates = get_candidates_by_score_range(min_score, max_score)
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
    Reject a candidate by updating their status to rejected
    """
    try:
        success, error_message = update_candidate_verdict(request.job_id, request.candidate_id, "rejected", request.verdict_comment)
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
    Accept a candidate by updating their status to accepted
    """
    try:
        success, error_message = update_candidate_verdict(request.job_id, request.candidate_id, "accepted", request.verdict_comment)
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

@router.post("/candidates/questions", response_model=QuestionsResponse)
async def generate_questions(request: QuestionsRequest):
    """
    Generate interview questions for a candidate based on job ID and candidate ID.
    The questions will be generated using ChatGPT and stored in S3.
    """
    try:
        candidate = get_candidate(request.job_id, request.candidate_id)
        if not candidate:
            raise HTTPException(
                status_code=404,
                detail=f"Candidate not found with job_id: {request.job_id} and candidate_id: {request.candidate_id}"
            )

        # Get the parsed resume key from candidate data
        s3_parsed_key = candidate.get('s3_parsed_key')
        if not s3_parsed_key:
            raise HTTPException(
                status_code=404,
                detail="Parsed resume not found for this candidate"
            )

        # Convert S3 key to URL
        parsed_resume_url = s3_key_to_url(s3_parsed_key)

        # Pass the URL to your LLM logic (if needed)
        questions = await generate_interview_questions(request.job_id, request.candidate_id, parsed_resume_url)

        # Store questions in S3 and get the key
        s3_key = f"{request.job_id}/{request.candidate_id}/questions.json"
        success = upload_to_s3(S3_BUCKET_NAME, questions, s3_key)
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to store questions in S3"
            )

        # Update DynamoDB with questions key
        success = update_candidate_questions(request.job_id, request.candidate_id, s3_key)
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to update candidate with questions key"
            )

        return {
            "s3_key": s3_key,
            "questions": questions
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate questions: {str(e)}"
        ) 