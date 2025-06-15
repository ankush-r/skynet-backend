from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.utils.aws_operations import get_candidate, get_json_from_s3, S3_BUCKET_NAME
from app.utils.llm_operations import generate_interview_questions

router = APIRouter()

class QuestionRequest(BaseModel):
    job_id: str
    candidate_id: str

@router.post("/questions")
async def generate_questions(request: QuestionRequest):
    try:
        # Get candidate details from DynamoDB
        candidate = get_candidate(request.job_id, request.candidate_id)
        if not candidate:
            raise HTTPException(status_code=404, detail="Candidate not found")
        
        # Get the S3 keys from candidate data
        s3_parsed_key = candidate.get('s3_parsed_key')
        resume_key = candidate.get('resume_key')
        
        # For testing, let's use a mock candidate analysis if keys are missing
        if not s3_parsed_key or not resume_key:
            print("Warning: Missing S3 keys, using mock data for testing")
            candidate_analysis = {
                "experience": "5 years of backend development experience",
                "skills": ["Python", "AWS", "Docker", "Kubernetes"],
                "education": "Bachelor's in Computer Science"
            }
        else:
            # Get content from S3
            candidate_analysis = get_json_from_s3(S3_BUCKET_NAME, s3_parsed_key)
            if not candidate_analysis:
                raise HTTPException(status_code=500, detail="Failed to fetch candidate analysis from S3")
        
        # Get job description from S3
        job_description = get_json_from_s3(S3_BUCKET_NAME, f"{request.job_id}/config/job-description.json")
        if not job_description:
            # For testing, use a mock job description
            print("Warning: Could not fetch job description, using mock data")
            job_description = {
                "title": "Backend Lead",
                "requirements": [
                    "5+ years of backend development",
                    "Experience with microservices",
                    "Strong AWS knowledge"
                ],
                "responsibilities": [
                    "Lead backend development team",
                    "Design and implement scalable architectures",
                    "Mentor junior developers"
                ]
            }
        
        # Generate questions using LLM
        questions = generate_interview_questions(job_description, candidate_analysis)
        
        if not questions:
            raise HTTPException(status_code=500, detail="Failed to generate questions")
        
        return {"questions": questions}
        
    except Exception as e:
        print(f"Error in generate_questions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 