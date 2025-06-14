import boto3
import json
import os
import uuid
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure AWS credentials from environment variables
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION', 'us-east-1')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')

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
        
        return items
    except Exception as e:
        print(f"Error querying DynamoDB: {str(e)}")
        return []

def update_candidate_verdict(candidate_id, verdict, verdict_comment):
    """
    Update a candidate's verdict and comment in DynamoDB
    
    Args:
        candidate_id (str): The ID of the candidate to update
        verdict (bool): The new verdict (True for accept, False for reject)
        verdict_comment (str): The comment explaining the verdict
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        dynamodb = session.client('dynamodb')
        
        # Update the item using the client
        response = dynamodb.update_item(
            TableName=os.getenv('DYNAMODB_TABLE_NAME'),
            Key={
                'id': {'S': candidate_id}
            },
            UpdateExpression='SET verdict = :v, verdict_comment = :c',
            ExpressionAttributeValues={
                ':v': {'BOOL': verdict},
                ':c': {'S': verdict_comment}
            },
            ReturnValues='UPDATED_NEW'
        )
        
        return True
    except Exception as e:
        print(f"Error updating candidate verdict: {str(e)}")
        return False

def add_item_to_dynamodb(item):
    """
    Add a new item to DynamoDB table
    
    Args:
        item (dict): The item to add to DynamoDB
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table(os.getenv('DYNAMODB_TABLE_NAME'))
        
        # Add the item to the table
        table.put_item(Item=item)
        return True
    except Exception as e:
        print(f"Error adding item to DynamoDB: {str(e)}")
        return False 