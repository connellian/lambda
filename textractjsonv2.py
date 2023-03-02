import json
import boto3

 


def lambda_handler(event, context):
    # Get the S3 bucket and key where the document was uploaded
    s3 = boto3.client("s3")
    input_bucket = event['Records'][0]['s3']['bucket']['name']
    input_key = event['Records'][0]['s3']['object']['key']

 

    # Call Amazon Textract to detect text in the document
    textract = boto3.client('textract', region_name='us-west-2')
    response = textract.start_document_text_detection(
        DocumentLocation={
            "S3Object": {
                "Bucket": input_bucket,
                "Name": input_key
            }
        }
    )
    
    # Wait for the text detection job to complete
    job_id = response["JobId"]
    result = None
    while result is None:
        response = textract.get_document_text_detection(JobId=job_id)
        status = response["JobStatus"]
        if status == "SUCCEEDED":
            result = response
        elif status == "FAILED":
            raise Exception("Textract job failed")
    
    # Convert the response to JSON format
    output = []
    for item in result["Blocks"]:
        if item["BlockType"] == "LINE":
            output.append(item["Text"])
    json_output = json.dumps(output)
    
    # Save the JSON output to another S3 bucket
    output_bucket = "s3-json-output-bucket-01"
    output_key = input_key.replace(".pdf", ".json") # Assumes the input file is a PDF
    s3.put_object(Bucket=output_bucket, Key=output_key, Body=json_output)
    
    return {
        "statusCode": 200,
        "body": json_output
    }
 
