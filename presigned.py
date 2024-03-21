import json
import boto3

def lambda_handler(event, context):
    bucket_name = 'testbucket978897'
    object_key = 'happy_birthday.png'
    pre_signed_url = generate_presigned_url(bucket_name, object_key)

    if pre_signed_url:
        print(f"Pre-signed URL for '{object_key}':\n{pre_signed_url}")
        send_presigned_url(pre_signed_url)
    else:
        print("Failed to generate pre-signed URL.")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    

def send_presigned_url(pre_signed_url):
    message = {"foo": "bar"}
    client = boto3.client('sns')
    response = client.publish(
        TargetArn='arn:aws:sns:us-east-1:975050338011:myTopic',
        Message=json.dumps({'default': json.dumps(pre_signed_url)}),
        Subject='a short subject for your message',
        MessageStructure='json'
    )
    

def generate_presigned_url(bucket_name, object_key, expiration_time=3600):
    """
    Generate a pre-signed URL for accessing an S3 object.

    :param bucket_name: The name of the S3 bucket.
    :param object_key: The key of the S3 object.
    :param expiration_time: The expiration time of the pre-signed URL in seconds (default is 1 hour).
    :return: The pre-signed URL.
    """
    # Create an S3 client with the signature version explicitly set to AWS4-HMAC-SHA256
    s3_client = boto3.client('s3', config=boto3.session.Config(signature_version='s3v4'), region_name='us-east-1')

    try:
        # Generate the pre-signed URL
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=expiration_time
        )

        return presigned_url

    except Exception as ex:
        print("Exception occured")
        return None
