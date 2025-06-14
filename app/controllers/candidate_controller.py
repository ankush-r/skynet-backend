from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from ..utils.aws_operations import get_candidates_by_score_range, update_candidate_verdict

router = APIRouter()

class VerdictRequest(BaseModel):
    id: str
    verdict_comment: str

@router.get("/candidates/range", response_model=List[dict])
async def get_candidates_in_range(min_score: Optional[int] = 45, max_score: Optional[int] = 55):
    """
    Get all candidates with scores between min_score and max_score
    """
    candidates = get_candidates_by_score_range(min_score, max_score)
    if candidates is None:
        raise HTTPException(status_code=500, detail="Error fetching candidates")
    return candidates

@router.post("/candidates/reject")
async def reject_candidate(request: VerdictRequest):
    """
    Reject a candidate by updating their verdict to False
    """
    success = update_candidate_verdict(request.id, False, request.verdict_comment)
    if not success:
        raise HTTPException(status_code=500, detail="Error updating candidate verdict")
    return {"message": "Candidate rejected successfully"}

@router.post("/candidates/accept")
async def accept_candidate(request: VerdictRequest):
    """
    Accept a candidate by updating their verdict to True
    """
    success = update_candidate_verdict(request.id, True, request.verdict_comment)
    if not success:
        raise HTTPException(status_code=500, detail="Error updating candidate verdict")
    return {"message": "Candidate accepted successfully"} 