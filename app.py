from __future__ import print_function
import os
from googleapiclient import errors, discovery
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from email.mime.text import MIMEText
import base64
import re
import requests
from datetime import date
from pytz import timezone
import boto3
import io
from oauth2client import client, tools, file
import httplib2

# TODO 
# 1. Validate the date of the email.
# 2. Delete email after process

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://mail.google.com/']
# ARK_INVEST_QUERY="from:ark@arkinvest.com subject:ARK Investment Management Trading Information"
ARK_INVEST_QUERY="from:ark@arkinvest.com"
S3_BUCKET ="ark-fly"
OBJECT_KEY_PATTERN="dailytradingtrans/{today}-trading.xls"
SEND_NOTIFICATION_TO = "guojiayanc@gmail.com"
SENDER = "noreply@arkfly.com"
TOKEN_FILE = "/tmp/token.pickle"

def main():
    service = login()
    try:
      result = get_messages(service, 'me')
      if 'messages' in result:
        messageId = result['messages'][0]['id']
        data = get_message('me', messageId, service)
        url = get_execel_url(data);
        # TODO fake url
        url = 'https://u4959697.ct.sendgrid.net/ls/click?upn=zcd6lV4HLKVOGeJ9ek2kSRXRFFQn1rBhvTyMa9aIC2TSfz2mEl5lAXMHUHfvzFMG7YOAWcdjUy4BIT0AMLXXeD-2B-2F8XE5vCf-2Fpdv9Ow71j94Z4w8mquL4MD-2FqTqhLd2suxURj_EeSb0sIe1Poi-2Ft5ye4CtICypLAQEKn7qP-2FXIqL-2F9ZqsULANVnV4NgyKEFNAKDSZ-2Bw7OvmGog7EQFmZ-2BPlZv8v-2Bt1MiaA4kCHw-2BzNNSMbCjMt-2Fj6ctOIGW2yAkK4-2BVlSGmYvYgOgMGiQRTrIqTEwmRAfp89JpUEX5Vr4X9mI3s4a2CACLCNOCS3i5Gfh90Wne42vYxiqfWujgjd6dSoJhsHTPxi-2FAg8sUuYLEdkbtJYU-2BfMwFdqs5zr8puiiiw6VGkPHOdDCgOsUFnpdJkfZFd0ZoNNJto1fX1TOq6s1aRx0-3D'
        response = requests.get(url)
        today = get_date()
        fileName = today + "-trading.xls"
        # with open(fileName, 'wb') as temp_file:
        #   temp_file.write(response.content)
        upload_to_s3(io.BytesIO(response.content), OBJECT_KEY_PATTERN.format(today=today))
      else:
        print("No message found!")
    except Exception as error:
        # TODO Sender is not set correctly
        print("ARK Fly processing failed " + str(error))
        message = create_message(SENDER, SEND_NOTIFICATION_TO, "ARK Fly processing failed", str(error))
        send_message(service, 'me', message)
def login():
  try:
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    return discovery.build('gmail', 'v1', http=http, cache_discovery=False)
  except Exception as error:
    print("Log into gmail failed!" + str(error))
    raise error

def get_credentials():
  client_id = os.environ['GMAIL_CLIENT_ID']
  client_secret = os.environ['GMAIL_CLIENT_SECRET']
  refresh_token = os.environ['GMAIL_REFRESH_TOKEN']
  credentials = client.GoogleCredentials(None, 
  client_id, 
  client_secret,
  refresh_token,
  None,
  "https://accounts.google.com/o/oauth2/token",
  'my-user-agent')

  return credentials

def upload_to_s3(content, object_name):
  client = boto3.client('s3')
  try:
    response = client.upload_fileobj(content, S3_BUCKET, object_name)
  except Exception as error:
    raise Exception("faile to upload to s3! " + str(error))

def get_date():
  tz = timezone('EST')
  today = date.today().strftime("%Y-%m-%d")
  return today

def get_message(user_id, message_id, service):
  try:
    message = service.users().messages().get(userId=user_id, id=message_id).execute()
    data = ''
    for part in message['payload']['parts']:
      data += base64.urlsafe_b64decode(part['body']['data']).decode("utf-8") + "\n"
    return data
  except Exception as error:
    raise error

def get_execel_url(data):
  updateUrl = re.search(r"Update your email preferences \(([^\)]*)\)", data)
  if updateUrl:
    return updateUrl.groups()[0]
  else:
    raise Exception("Today's trading URL not found!")

def create_message(sender, to, subject, message_text):
  message = MIMEText(message_text)
  message['to'] = to
  message['from'] = sender
  message['subject'] = subject
  raw_message = base64.urlsafe_b64encode(message.as_string().encode("utf-8"))
  return {
    'raw': raw_message.decode("utf-8")
  }

def send_message(service, user_id, message):
  try:
    message = service.users().messages().send(userId=user_id, body=message).execute()
    print('Message Id: %s' % message['id'])
    return message
  except Exception as error:
    print('An error occurred: %s' % error)
    raise error

def get_messages(service, user_id):
  try:
    return service.users().messages().list(userId=user_id, q=ARK_INVEST_QUERY).execute()
  except Exception as error:
    print('An error occurred: %s' % error)
    raise error

# For local test
if __name__ == '__main__':
    main()

def lambda_handler(event, context):
  main()
  return {
    "status":200
  }

