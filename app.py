import os
from googleapiclient import errors, discovery
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from email.mime.text import MIMEText
import base64
import re
import requests
from datetime import date, datetime
from pytz import timezone
import boto3
from oauth2client import client, tools, file
import httplib2
from bs4 import BeautifulSoup
import csv

# TODO
# 1. Validate the date of the email.
# 2. Delete email after process

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://mail.google.com/']
# ARK_INVEST_QUERY="from:ARK Trading Desk <ark@ark-funds.com> subject:ARK Investment Management LLC - * - Daily Trade Information*"
ARK_INVEST_QUERY=os.environ['EMAIL_QUERY']
S3_BUCKET ="ark-fly"
OBJECT_KEY_PATTERN="dailytradingtrans/{today}-trading.csv"
SEND_NOTIFICATION_TO = "guojiayanc@gmail.com"
SENDER = "noreply@arkfly.com"
TEMP_TRADING_CSV_FILE="/tmp/trading.csv"

def main():
  service = login()
  try:
    result = list_messages(service, 'me')
    if 'messages' in result:
      messageId = result['messages'][0]['id']
      data = get_message('me', messageId, service)
      generate_csv(data)
      today = get_date()
      upload_to_s3(OBJECT_KEY_PATTERN.format(today=today))
      delete_massage(service, 'me', messageId)
    else:
      print("No message found!")
  except Exception as error:
    # TODO Sender is not set correctly
    print("ARK Fly processing failed " + str(error))
    message = create_message(SENDER, SEND_NOTIFICATION_TO, "ARK Fly processing failed", str(error))
    send_message(service, 'me', message)

def delete_massage(service, user_id, message_id):
  try:
    message = service.users().messages().delete(userId=user_id, id=message_id).execute()
  except Exception as error:
    raise error

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

def upload_to_s3(object_name):
  client = boto3.client('s3')
  try:
    response = client.upload_file(TEMP_TRADING_CSV_FILE, S3_BUCKET, object_name)
  except Exception as error:
    raise Exception("faile to upload to s3! " + str(error))

def get_date():
  tz = timezone('EST')
  today = datetime.now(tz).strftime("%Y-%m-%d")
  return today

def get_message(user_id, message_id, service):
  try:
    message = service.users().messages().get(userId=user_id, id=message_id).execute()
    data = ''
    if 'parts' in message['payload']:
      for part in message['payload']['parts']:
        data += base64.urlsafe_b64decode(part['body']['data']).decode("utf-8") + "\n"
    else:
      data += base64.urlsafe_b64decode(message['payload']['body']['data']).decode("utf-8") + "\n"
    return data
  except Exception as error:
    raise error

def generate_csv(data):
  try:
    bs=BeautifulSoup(data, 'html.parser')
    table_container=bs.find('td', attrs={'role':'modules-container'})
    root_tables = table_container.find_all('table', class_=False)
    csv_rows = []
    csv_header = ['Fund', 'Date', 'Direction', 'Ticker', 'Company', 'Shares', '% of ETF']
    csv_rows.append(csv_header)
    for root_table in root_tables:
      if root_table.find('tbody').find('table'):
        tables = root_table.find_all('table')
        etf = ''
        trade_date = ''
        for table in tables:
          rows = table.find_all('tr')
          for row in rows:
            td = row.find_all('td')
            if len(td) == 2:
              etf = td[0].text.strip().split()[0]
              trade_date = td[1].text.strip()
           
            elif len(td) == 4:
    
              cols=[x.text.strip().replace('\r\n', ' ') for x in td]
              if cols[0] != 'Direction':
                sharesAndPercentage = cols[3].split('|')
                cols.pop(3)
                cols.append(sharesAndPercentage[0].strip())
                cols.append(sharesAndPercentage[1].strip())
                cols.insert(0, trade_date)
                cols.insert(0, etf)
                csv_rows.append(cols)
    print(csv_rows)
    with open(TEMP_TRADING_CSV_FILE, "w") as f:
      wr = csv.writer(f)
      wr.writerows(csv_rows)
  except Exception as error:
    raise Exception("Today's trading table not found!" + str(error))

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

def list_messages(service, user_id):
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
