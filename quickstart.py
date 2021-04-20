from __future__ import print_function
import pandas as pd
import os.path
import io
import requests
from io import StringIO
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient import errors
from googleapiclient.http import MediaIoBaseDownload
from sqlalchemy import create_engine
import pymysql

FOLDER_ID = "13mJ8zPtU3uvNTV0yhIfSz-0Sfh0IMaMl"

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def get_files(service,folder_id):
    results = service.files().list(
        q="mimeType='text/csv' and parents in '"+folder_id+"' and trashed = false",
        fields="nextPageToken, files(id, name)",pageSize=10).execute()
    items = results.get('files', [])

    if not items:
        print('No files found.\n') 
    else:
        print('Files:')
        for item in items:
            print(u'{0} ({1})'.format(item['name'], item['id']))
        print('\n')

def import_file_to_df(service, file_id):
  path = 'https://drive.google.com/uc?export=download&id='+file_id
  url = requests.get(path).text
  file_name = StringIO(url)
  df = pd.read_csv(path)

  print(df.head())
  return df

def send_to_database(df):
    tableName = "sample" ##Can take user input for the name of the table to be created
    host = "127.0.0.1"
    password = "ironman"
    db = "trial"
    uname = "root"

    sqlEngine = create_engine('mysql+pymysql://{user}:{pwd}@{hostname}/{dbname}'.format(user = uname, pwd = password, hostname = host, dbname = db))

    dbConnection = sqlEngine.connect()

    try:
        frame = df.to_sql(tableName, dbConnection, if_exists='fail')

    except ValueError as vx:
        print(vx)

    except Exception as ex:   
        print(ex)

    else:
        print("Table %s created successfully."%tableName);   
    
    finally:
        dbConnection.close()


def getAllCSVFiles(service,folder_id):  
    results = service.files().list(q="mimeType='text/csv' and parents in '"+folder_id+"' and trashed = false",fields="nextPageToken, files(id, name, createdTime)", orderBy="createdTime").execute()
    items = results.get('files', [])

    if not items:
        return ""
    else:
        return items

def retrieveLatestFile(items,count):  
    if not items:
        return ""
    else:
        return items[-1]

def countFiles(service,folder_id):

    results = service.files().list(q="parents in '"+folder_id+"' and trashed = false",fields="nextPageToken, files(id, name)").execute()

    items = results.get('files', [])

    count = 0
    for item in items:
        count += 1
    return count

def main():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('drive', 'v3', credentials=creds)

    #Print all files and folders in current directory
    get_files(service, FOLDER_ID)

    #Get number of files present in the folder
    file_count = countFiles(service,FOLDER_ID)
    print(u'{0} number of files present in this folder\n'.format(file_count))

    #Get the name and id of the latest file uploaded
    files = getAllCSVFiles(service,FOLDER_ID)

    latestFile = retrieveLatestFile(files,file_count)
    print(latestFile['name'] + "(" + latestFile['id'] + ")\n")

    id = latestFile['id'] ## Retrieve id of the latest uploaded file

    #import data from the file to a dataframe and store in a database in the form of a table
    df = import_file_to_df(service,id)
    send_to_database(df)

if __name__ == '__main__':
    main()