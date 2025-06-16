import boto3
import json
import os
import uuid
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Check required environment variables
required_env_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION', 'S3_BUCKET_NAME', 'DYNAMODB_TABLE_NAME']
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Export bucket and table names
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
DYNAMODB_TABLE_NAME = os.getenv('DYNAMODB_TABLE_NAME')

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

def upload_to_s3(bucket_name, file_content, key):
    """
    Upload JSON content to S3 bucket with a specific key
    
    Args:
        bucket_name (str): Name of the S3 bucket
        file_content (dict/str): JSON content to upload (can be dict or JSON string)
        key (str): Specific S3 key to use
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        s3 = session.client('s3')
        
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
                return False
        else:
            print("Invalid content type. Expected dict or JSON string.")
            return False
        
        # Upload to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json_content,
            ContentType='application/json'
        )
        
        return True
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        return False

def get_candidates_by_score_range(min_score=0, max_score=100, status='IN_CONSIDERATION'):
    """
    Get all candidates from DynamoDB whose absolute score is between min_score and max_score
    and have the specified status
    
    Args:
        min_score (int): Minimum absolute score (default: 0)
        max_score (int): Maximum absolute score (default: 100)
        status (str): Status of the candidate (default: 'IN_CONSIDERATION')
        
    Returns:
        list: List of candidate items matching the score range and status
    """
    try:
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table(os.getenv('DYNAMODB_TABLE_NAME'))
        
        # Create filter expression for score range and status
        filter_expression = 'attribute_exists(absolute_score) AND absolute_score BETWEEN :min_score AND :max_score AND #status = :status'
        expression_values = {
            ':min_score': min_score,
            ':max_score': max_score,
            ':status': status
        }
        expression_names = {
            '#status': 'status'  # status is a reserved word in DynamoDB
        }
        
        # Query the table
        response = table.scan(
            FilterExpression=filter_expression,
            ExpressionAttributeValues=expression_values,
            ExpressionAttributeNames=expression_names
        )
        
        # Get all items
        items = response.get('Items', [])
        
        # Handle pagination if there are more results
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression=filter_expression,
                ExpressionAttributeValues=expression_values,
                ExpressionAttributeNames=expression_names,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
        
        return items
    except Exception as e:
        print(f"Error querying DynamoDB: {str(e)}")
        return []

def get_candidate(job_id, candidate_id):
    """
    Get a candidate from DynamoDB by job_id and candidate_id
    
    Args:
        job_id (str): The job ID (partition key)
        candidate_id (str): The candidate ID (sort key)
        
    Returns:
        dict: The candidate item if found, None otherwise
    """
    try:
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table(os.getenv('DYNAMODB_TABLE_NAME'))
        
        response = table.get_item(
            Key={
                'job_id': job_id,
                'candidate_id': candidate_id
            }
        )
        print(response.get('Item'))
        
        return response.get('Item')
    except Exception as e:
        print(f"Error getting candidate: {str(e)}")
        return None

def update_candidate_verdict(job_id, candidate_id, status, verdict_comment):
    """
    Update a candidate's status and comment in DynamoDB
    
    Args:
        job_id (str): The job ID (partition key)
        candidate_id (str): The candidate ID (sort key)
        status (str): The new status (accepted/rejected)
        verdict_comment (str): The comment explaining the verdict
        
    Returns:
        tuple: (bool, str) - (success, error_message)
    """
    try:
        # First check if the candidate exists
        candidate = get_candidate(job_id, candidate_id)
        if not candidate:
            return False, f"Candidate not found with job_id: {job_id} and candidate_id: {candidate_id}"
        
        dynamodb = session.client('dynamodb')
        
        # Update the item using the client
        response = dynamodb.update_item(
            TableName=os.getenv('DYNAMODB_TABLE_NAME'),
            Key={
                'job_id': {'S': job_id},
                'candidate_id': {'S': candidate_id}
            },
            UpdateExpression='SET #status = :s, verdict_comment = :c',
            ExpressionAttributeNames={
                '#status': 'status'  # status is a reserved word in DynamoDB
            },
            ExpressionAttributeValues={
                ':s': {'S': status},
                ':c': {'S': verdict_comment}
            },
            ReturnValues='UPDATED_NEW'
        )
        
        return True, "Success"
    except Exception as e:
        error_msg = f"Error updating candidate status: {str(e)}"
        print(error_msg)
        return False, error_msg

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

def get_all_candidates_by_job_id(job_id):
    """
    Get all candidates from DynamoDB for a specific job_id, sorted by:
    1. absolute_score (descending)
    2. jd_score (descending)
    3. cultural_fit_score (descending)
    4. uniqueness_score (descending)
    
    Only returns candidates with status 'ACCEPTED' or 'IN_CONSIDERATION'
    
    Args:
        job_id (str): The job ID to filter candidates by
        
    Returns:
        list: List of candidate items for the specified job, sorted by scores
    """
    try:
        # Validate AWS configuration
        if not all([aws_access_key, aws_secret_key, aws_region, DYNAMODB_TABLE_NAME]):
            print("❌ AWS configuration is incomplete. Please check your .env file.")
            return None

        print(f"\n🔍 Fetching candidates for job_id: {job_id}")
        print(f"Using DynamoDB table: {DYNAMODB_TABLE_NAME}")
        print(f"AWS Region: {aws_region}")
        
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)
        
        # Query the table using job_id as the partition key and filter by status
        print("Executing DynamoDB query...")
        response = table.query(
            KeyConditionExpression='job_id = :job_id',
            FilterExpression='#status IN (:status1, :status2)',
            ExpressionAttributeNames={
                '#status': 'status'  # status is a reserved word in DynamoDB
            },
            ExpressionAttributeValues={
                ':job_id': job_id,
                ':status1': 'ACCEPTED',
                ':status2': 'IN_CONSIDERATION'
            }
        )
        
        # Get all items
        items = response.get('Items', [])
        print(f"Found {len(items)} candidates")
        
        # Handle pagination if there are more results
        while 'LastEvaluatedKey' in response:
            print("Fetching more results...")
            response = table.query(
                KeyConditionExpression='job_id = :job_id',
                FilterExpression='#status IN (:status1, :status2)',
                ExpressionAttributeNames={
                    '#status': 'status'
                },
                ExpressionAttributeValues={
                    ':job_id': job_id,
                    ':status1': 'ACCEPTED',
                    ':status2': 'IN_CONSIDERATION'
                },
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
            print(f"Total candidates found: {len(items)}")
        
        if not items:
            print("No candidates found in DynamoDB")
            return []
            
        # Sort the items based on multiple criteria
        print("Sorting candidates by scores...")
        sorted_items = sorted(
            items,
            key=lambda x: (
                -x.get('absolute_score', 0),  # Negative for descending order
                -x.get('jd_score', 0),
                -x.get('cultural_fit_score', 0),
                -x.get('uniqueness_score', 0)
            )
        )
        
        print(f"Sorted {len(sorted_items)} candidates by scores")
        return sorted_items
    except Exception as e:
        print(f"❌ Error querying DynamoDB: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return None

def update_candidate_questions(job_id, candidate_id, questions_key):
    """
    Update a candidate's questions key in DynamoDB
    
    Args:
        job_id (str): The job ID (partition key)
        candidate_id (str): The candidate ID (sort key)
        questions_key (str): The S3 key where questions are stored
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        dynamodb = session.client('dynamodb')
        
        # Update the item using the client
        response = dynamodb.update_item(
            TableName=os.getenv('DYNAMODB_TABLE_NAME'),
            Key={
                'job_id': {'S': job_id},
                'candidate_id': {'S': candidate_id}
            },
            UpdateExpression='SET questions = :q',
            ExpressionAttributeValues={
                ':q': {'S': questions_key}
            },
            ReturnValues='UPDATED_NEW'
        )
        
        return True
    except Exception as e:
        print(f"Error updating candidate questions: {str(e)}")
        return False

def s3_key_to_url(key, bucket_name=None, region=None):
    bucket = bucket_name or S3_BUCKET_NAME
    region = region or aws_region
    if region == "us-east-1":
        return f"https://{bucket}.s3.amazonaws.com/{key}"
    else:
        return f"https://{bucket}.s3.{region}.amazonaws.com/{key}" 