import os
from googleapiclient import errors, discovery
from oauth2client import client, tools, file

SCOPES = ['https://mail.google.com/']
def get_credentials():
    wd = os.getcwd()
    
    # creates credentials with a refresh token
    credential_path = os.path.join(wd,
                                  'credentials_out.json')
    store = file.Storage(credential_path)
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('client_secret.json', SCOPES)
        creds = tools.run_flow(flow, store)
    return creds

if __name__ == '__main__':
    get_credentials()