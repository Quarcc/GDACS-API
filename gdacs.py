import asyncio
import os.path
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta
from aio_georss_gdacs import GdacsFeed
from aiohttp import ClientSession
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

SPREADSHEET_ID = '1rINCb4G7RExZnw6_S9mQGHSHEaHIXq4b3f26QD21dHc'


def main():
    creds = None
    print('\nConnecting to Google Sheets API')
    # Load token file, if doesnt exist, run code with credentials.json
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If credentials not valid
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    print('\nSuccessfully connected to Google Sheets API')

    try:

        service = build("sheets", "v4", credentials=creds)
        sheets = service.spreadsheets()

        service.spreadsheets().values().clear(spreadsheetId=SPREADSHEET_ID, range='A2:Z1000').execute()

        start = datetime.now()
        start = start - relativedelta(months=24)

        step = 730

        res = []

        for day in range(step):
            date = (start + timedelta(days=day)).strftime('%Y-%m-%d')
            res.append(date)
        data_list = []
        print('\nRunning GDACS API, Retrieving Information')
        while True:
            async def gdacs() -> None:
                async with ClientSession() as websession:
                    # Home Coordinates: Latitude: -33.0, Longitude: 150.0
                    feed = GdacsFeed(websession,
                                     (-33.0, 150.0))
                    status, entries = await feed.update()
                    for i in entries:
                        if i.from_date.strftime('%Y-%m-%d') in res and i.country is not None:
                            a = i.country.split(', ')
                            for j in a:
                                data = {}
                                data['country'] = j
                                data['date'] = i.from_date.strftime('%Y-%m-%d')
                                data['event'] = i.event_type
                                data_list.append(data)
            asyncio.get_event_loop().run_until_complete(gdacs())
            break
        
        final = []
        for i in range(len(data_list)):
            if data_list[i] not in final:
                final.append(data_list[i])
            else:
                pass
            
        length = 0
        r = 2
        i = 0
        print('\nUpdating Google Sheets with new data, this may take a while...(up to 10 minutes)')
        while length < len(final):
            try:
                print(f'{i+1}/{len(final)}')

                sheets.values().update(spreadsheetId=SPREADSHEET_ID, range=f'Sheet1!A{r}', valueInputOption='USER_ENTERED', body={'values': [[final[i]['country']]]}).execute()
                sheets.values().update(spreadsheetId=SPREADSHEET_ID, range=f'Sheet1!B{r}', valueInputOption='USER_ENTERED', body={'values': [[final[i]['date']]]}).execute()
                sheets.values().update(spreadsheetId=SPREADSHEET_ID, range=f'Sheet1!C{r}', valueInputOption='USER_ENTERED', body={'values': [[final[i]['event']]]}).execute()
                r += 1
                i += 1
                length += 1
            except HttpError:
                continue
    except HttpError as error:
        print(error)


if __name__ == "__main__":
    print('Running GDACS.py...')
    main()
