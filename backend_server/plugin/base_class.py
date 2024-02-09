from abc import ABC, abstractmethod
from base64 import b64encode
from datetime import date
from azure.storage.blob import BlobServiceClient
import aiohttp
import json


class BaseScraper(ABC):
    def __init__(self, base_url, client_id, client_secret):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = ''


    async def fetch_api_token(self):
        auth_url = 'https://accounts.spotify.com/api/token'
        credentials = b64encode(f'{self.client_id}:{self.client_secret}'.encode()).decode('utf-8')
        auth_data = {'grant_type': 'client_credentials'}
        auth_headers = {'Authorization': f'Basic {credentials}'}

        async with aiohttp.ClientSession() as session:
            async with session.post(auth_url, data=auth_data, headers=auth_headers) as response:
                if response.status == 200:
                    data = await response.json()
                    access_token = data['access_token']
                    self.access_token = access_token
                else:
                    print(f'Access Token Error {response.status}, {await response.text()}')


    async def fetch_data_from_api(self, session, params, api_url):
        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

        async with session.get(api_url, headers=headers, params=params) as response:
            if response.status != 200:
                print(f'Error occurred: {response.status} {response}')
                return {}
            response_text = await response.text(encoding='utf-8')
            data = json.loads(response_text)

            return data


    @abstractmethod
    async def main(self):
        pass


    @staticmethod
    def save_json(file_name, results):
        file_path = f'./static/{file_name}.json'
        result = {'results': results}

        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(result, json_file, ensure_ascii=False, indent=4)
        
        return file_path


    @staticmethod
    def upload_to_blob(file_path, title, account_name, account_key, container_name):
        today = date.today()
        year = str(today.year)
        month = str(today.month).zfill(2)
        day = str(today.day).zfill(2)

        blob_path = f'{title}/year={year}/month={month}/day={day}/{title}.json'

        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key
        )

        container_client = blob_service_client.get_container_client(container_name)

        with open(file_path, 'rb') as data:
            container_client.upload_blob(name=blob_path, data=data)

        print(f'End Upload to {container_name} container {blob_path} blob.')
