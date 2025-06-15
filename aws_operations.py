import boto3
import json
import os
import uuid
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure AWS credentials
aws_access_key = 'ASIAXKKU4AB4U4GUI6EB'
aws_secret_key = 'eDp1kFQ+7urL1fNz0mDIbJlEoi1rjUYy5WNKdvmo'
aws_region = 'us-east-1'
aws_session_token = 'IQoJb3JpZ2luX2VjEEgaCXVzLWVhc3QtMSJHMEUCIQC5Y1tkJCL1fYZ8pHXimyvSkgxVpI6CkNfNLpL3R2JKOgIgXIUEj3dlm61UQvUgGY/WBZjg13VKaBH0y4Nn8dsezUIqnwMIMBADGgw1MDMyMjYwNDA0NDEiDFgTiZAMhhfn/5sNaSr8AkfeI4lbIjgFinilryeAd/06KenWKkA0TR0y0S4vgVE69DcquwHk++ap5WCQVaCUMhve8uBHULkk1fhYm3sDmV3aqKvuqcwF99QaQtUnNSS1KEazVjTDgb4YI9Ok9r8udYeL5B8ZW57c/ChwmddN9n3wMfY/og1BSlHmZiluJuNaDIv5PtuLv343vMvoIOb6UlHrXeVa7oNV9agXYsgwvcdzTO9l33j0ASQW+O6fJjnocDWPdqcah9XQQxiOff79owVhMCBbtCUSl9ZhVjzkA6WFdem8QIYopI2oXh1obqvEYt+emKOoYEIRbp8r6mwIhuUoFPFNCXJh8UGIfe5TuhtprVp3XfqhBAAOZOVub5fcjcVZfvd8tJrLID34+6ziFQhHRt+JWbzc1JWAmVgZI1T6FrZzOPgeGBU61SHXR6JskzigQXp1OOI1EGi6Iv3fmSLiZwVBj5Skmgw3g4peORZ9/dGQVPZMYjpUdDU1YquxwZBs3O6qua7Zumi2MNeftsIGOqYBrAi8yfbLmE1nNIyk7qxO8PbFxK5cpG32BJ7IOLl3TJXqG4wtcvYt05YIdsT2FFwhNVPKNTWDpc/DmQ/uYX8ykRWDEKMCa2fLq9h2kxtbTB/gfnTymPrUyLOosagmnojHnCkotwn3T8zGqQd/br4nLPHqKLUGCmGYgAvRN8uiulG1pSsagqaf9BLs1scmv948ElSVuZ8eQI/kaXNvaou/IQJRWLxLMg=='
# Debug logging for AWS credentials
print("\n🔑 AWS Configuration:")
print(f"Region: {aws_region}")
print(f"Access Key: {aws_access_key[:5]}...{aws_access_key[-4:]}")
print(f"Secret Key: {'*' * 5}...{'*' * 4} (hidden)")
print(f"Session Token: {'*' * 5}...{'*' * 4} (hidden)")
print(f"Bucket Name: {os.getenv('S3_BUCKET_NAME')}")
print(f"DynamoDB Table: {os.getenv('DYNAMODB_TABLE_NAME')}\n")

if not all([aws_access_key, aws_secret_key, aws_region, aws_session_token]):
    raise ValueError("Missing required AWS credentials in environment variables")

# Configure AWS session
session = boto3.Session(
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    aws_session_token=aws_session_token,
    region_name=aws_region
)

def get_json_from_s3(bucket_name, key):
    """
    Fetch JSON content from S3 bucket
    
    Args:
        bucket_name (str): Name of the S3 bucket
        key (str): Key/path of the object in S3
        
    Returns:
        dict: Parsed JSON content or None if error
    """
    try:
        s3 = session.client('s3')
        response = s3.get_object(Bucket=bucket_name, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        print(f"Error fetching JSON from S3: {str(e)}")
        return None

def upload_to_s3(bucket_name, file_content, prefix):
    """
    Upload JSON content to S3 bucket with a prefix
    
    Args:
        bucket_name (str): Name of the S3 bucket
        file_content (dict/str): JSON content to upload (can be dict or JSON string)
        prefix (str): Prefix for the S3 key
        
    Returns:
        str: S3 URL of the uploaded object or None if error
    """
    try:
        s3 = session.client('s3')
        
        # Generate a unique filename using timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{prefix}_{timestamp}.json"
        
        # Handle different input types and ensure valid JSON
        if isinstance(file_content, dict):
            json_content = json.dumps(file_content, indent=2)
        elif isinstance(file_content, str):
            # Validate if string is valid JSON
            try:
                # Parse and re-stringify to ensure valid JSON
                parsed_json = json.loads(file_content)
                json_content = json.dumps(parsed_json, indent=2)
            except json.JSONDecodeError as e:
                print(f"Invalid JSON string provided: {str(e)}")
                return None
        else:
            print("Invalid content type. Expected dict or JSON string.")
            return None
        
        # Upload to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json_content,
            ContentType='application/json'
        )
        
        # Generate and return the S3 URL
        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{filename}"
        return s3_url
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        return None

def add_to_dynamodb(item_data):
    """
    Add a new item to DynamoDB table
    
    Args:
        item_data (dict): Dictionary containing the item data
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table(os.getenv('DYNAMODB_TABLE_NAME'))
        
        # Generate a unique ID if not provided
        if 'id' not in item_data:
            item_data['id'] = str(uuid.uuid4())
            
        # Add timestamp
        item_data['created_at'] = datetime.now().isoformat()
        
        # Ensure all required attributes are present
        required_attributes = [
            'name', 'email', 'absolute_scoring', 'jd_score', 
            'culture_score', 'verdict', 'verdict_comment',
            'uniqueness_score', 'resume_url', 'parsed_url',
            'jd_analysis_url', 'culture_analysis_url'
        ]
        
        for attr in required_attributes:
            if attr not in item_data:
                item_data[attr] = None
        
        # Add item to DynamoDB
        table.put_item(Item=item_data)
        return True
    except Exception as e:
        print(f"Error adding item to DynamoDB: {str(e)}")
        return False

def get_candidates_by_score_range(min_score=45, max_score=55):
    """
    Get all candidates from DynamoDB whose absolute scoring is between min_score and max_score
    
    Args:
        min_score (int): Minimum absolute score (default: 45)
        max_score (int): Maximum absolute score (default: 55)
        
    Returns:
        list: List of candidate items matching the score range
    """
    try:
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table(os.getenv('DYNAMODB_TABLE_NAME'))
        
        # Create filter expression for score range
        filter_expression = 'absolute_scoring BETWEEN :min_score AND :max_score'
        expression_values = {
            ':min_score': min_score,
            ':max_score': max_score
        }
        
        # Query the table
        response = table.scan(
            FilterExpression=filter_expression,
            ExpressionAttributeValues=expression_values
        )
        
        # Get all items
        items = response.get('Items', [])
        
        # Handle pagination if there are more results
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression=filter_expression,
                ExpressionAttributeValues=expression_values,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
        
        print(f"\n📊 Found {len(items)} candidates with scores between {min_score} and {max_score}")
        return items
    except Exception as e:
        print(f"Error querying DynamoDB: {str(e)}")
        return []

# Example usage
if __name__ == "__main__":
    # Example of getting JSON from S3
    bucket_name = "team-skynet-s3-bucket"
    # key = "parsed-resume/Abhinav_Dhasmana_parsed.json"
    
    # print(f"\n📥 Fetching JSON from S3:")
    # print(f"Bucket: {bucket_name}")
    # print(f"Key: {key}")
    
    # json_data = get_json_from_s3(bucket_name, key)
    
    # if json_data:
    #     print("\n✅ Successfully fetched JSON data")
    #     print(json.dumps(json_data, indent=2))
    # else:
    #     print("\n❌ Failed to fetch JSON data")
    
    # # Example of getting candidates by score range
    # print("\n🔍 Fetching candidates with scores between 45 and 55:")
    # candidates = get_candidates_by_score_range(45, 90)
    # if candidates:
    #     print("\nFound candidates:")
    #     for candidate in candidates:
    #         print(f"\nName: {candidate.get('name')}")
    #         print(f"Email: {candidate.get('email')}")
    #         print(f"Absolute Score: {candidate.get('absolute_scoring')}")
    #         print(f"JD Score: {candidate.get('jd_score')}")
    #         print(f"Culture Score: {candidate.get('culture_score')}")
    #         print(f"Verdict: {candidate.get('verdict')}")
    #         print("-" * 50)
    
    # Example of uploading to S3
    # content = {"example": "data"}
    # s3_url = upload_to_s3(bucket_name, content, "prefix")
    
    # Example of adding to DynamoDB
    item = {
        "name": "John Doe",
        "email": "john@example.com",
        "absolute_scoring": 85,
        "jd_score": 90,
        "culture_score": 88,
        "verdict": True,
        "verdict_comment": "Strong candidate",
        "uniqueness_score": 92,
        "resume_url": "https://example.com/resume.pdf",
        "parsed_url": "https://example.com/parsed.json",
        "jd_analysis_url": "https://example.com/jd_analysis.json",
        "culture_analysis_url": "https://example.com/culture_analysis.json"
    }
    success = add_to_dynamodb(item)