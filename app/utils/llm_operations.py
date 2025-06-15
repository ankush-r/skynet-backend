from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Check for OpenAI API key
if not os.getenv('OPENAI_API_KEY'):
    raise EnvironmentError("OPENAI_API_KEY environment variable is not set")

# Initialize LLM
llm = ChatOpenAI(
    model="gpt-4",
    temperature=0.5,
    api_key=os.getenv('OPENAI_API_KEY')
)

def generate_interview_questions(job_description, candidate_analysis):
    """
    Generate interview questions using GPT-4 based on job description and candidate analysis
    
    Args:
        job_description (dict/str): The job description content
        candidate_analysis (dict/str): The candidate's analysis/resume content
        
    Returns:
        list: List of question objects with question, category, and context
    """
    # Convert inputs to strings if they are dictionaries
    if isinstance(job_description, dict):
        job_description = json.dumps(job_description, indent=2)
    if isinstance(candidate_analysis, dict):
        candidate_analysis = json.dumps(candidate_analysis, indent=2)
    
    prompt = f"""
        Generate 3-5 interview questions for a candidate based on the following information:
        
        Job Description:
        {job_description}
        
        Candidate Experience:
        {candidate_analysis}
        
        Please generate questions in the following categories:
        1. Job Description Based: Questions that assess the candidate's understanding and fit for the role
        2. Experience Based: Questions that explore the candidate's past experience and achievements
        3. Trending Topics: Questions about current industry trends and technologies
        
        For each question, provide:
        - The question text
        - The category (jd_based, experience_based, or trending)
        - A brief context explaining why this question is relevant
        
        Format the response as a JSON array of objects with 'question', 'category', and 'context' fields.
        Example format:
        [
            {{
                "question": "How would you approach implementing a microservices architecture?",
                "category": "jd_based",
                "context": "This question assesses the candidate's understanding of modern software architecture."
            }}
        ]
        
        Ensure the response is valid JSON and includes 3-5 questions across different categories.
        """
    
    try:
        messages = [HumanMessage(content=prompt)]
        response = llm.invoke(messages)
        
        # Parse the response content as JSON
        questions = json.loads(response.content)
        return questions
    except json.JSONDecodeError as e:
        print(f"Error parsing LLM response as JSON: {str(e)}")
        print(f"Raw response: {response.content}")
        return []
    except Exception as e:
        print(f"Error generating questions: {str(e)}")
        return []
