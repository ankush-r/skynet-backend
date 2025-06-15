import os
import json
from typing import List, Dict, Any
from datetime import datetime
import openai
from fastapi import HTTPException
from botocore.exceptions import ClientError
from .aws_operations import session, S3_BUCKET_NAME, DYNAMODB_TABLE_NAME

# Check required environment variables
required_env_vars = ['OPENAI_API_KEY', 'S3_BUCKET_NAME']
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Initialize AWS clients using the session from aws_operations
dynamodb = session.resource('dynamodb')
s3_client = session.client('s3')

async def generate_interview_questions(job_id: str, candidate_id: str, s3_parsed_key: str) -> Dict[str, Any]:
    """
    Generate interview questions using ChatGPT API based on parsed resume.
    The questions will be stored in S3 and the key will be returned.
    
    Args:
        job_id (str): The ID of the job posting
        candidate_id (str): The ID of the candidate
        s3_parsed_key (str): The S3 key of the parsed resume
        
    Returns:
        Dict[str, Any]: Dictionary containing the questions
    """
    try:
        if not S3_BUCKET_NAME:
            raise HTTPException(
                status_code=500,
                detail="S3_BUCKET_NAME environment variable is not configured"
            )

        # Get parsed resume from S3
        s3 = session.client('s3')
        try:
            response = s3.get_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_parsed_key
            )
            parsed_resume = json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to fetch parsed resume from S3: {str(e)}"
            )

        # Prepare the prompt for ChatGPT
        prompt = f"""Based on the following parsed resume, generate interview questions.
        The questions should be specific to the candidate's experience and skills.
        
        Parsed Resume:
        {json.dumps(parsed_resume, indent=2)}
        
        Generate 3-5 questions in each of these categories:
        1. Technical Leadership & Architecture
        2. Job description skills
        3. resume experience skills
        4. Team Management & Communication
        5. Problem Solving & System Design
        
        Format the response as a JSON object with these categories as keys and arrays of questions as values.
        Each question should be specific and reference the candidate's experience or skills.
        """

        # Call ChatGPT API
        try:
            client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
            response = client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert technical interviewer. Generate specific, relevant interview questions based on the candidate's experience and skills."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=2000
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate questions with OpenAI: {str(e)}"
            )

        # Extract and parse the questions
        try:
            content = response.choices[0].message.content
            questions = json.loads(content)
        except (json.JSONDecodeError, AttributeError) as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to parse OpenAI response: {str(e)}"
            )

        return questions

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error generating questions: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate interview questions: {str(e)}"
        ) 