from base64 import b64encode
from datetime import date
from azure.storage.blob import BlobServiceClient
import aiohttp
import json


class ArtistTopTrackScraper:
    kpop_girl_group_category_dict = {
        'EXID': '1xs6WFotNQSXweo0GXrS0O',
        'TWICE': '7n2Ycct7Beij7Dj7meI4X0',
        'BLACKPINK': '41MozSoPIsD1dJM0CLPjZF',
        'MAMAMOO': '0XATRDCYuuGhk0oE7C0o5G',
        'Red Velvet': '1z4g3DjTBBZKhvAroFlhOM',
        'OH MY GIRL': '2019zR22qK2RBvCqtudBaI',
        'fromis_9': '24nUVBIlCGi4twz4nYxJum',
        '(G)I-DLE': '2AfmfGFbe0A0WsTYm0SDTx',
        'ITZY': '2KC9Qb60EaY0kW4eH68vr3',
        'STAYC': '01XYiBYaoMJcNhPokrg0l0',
        'aespa': '6YVMFz59CuY7ngCxTxjpxE',
        'IVE': '6RHTUrRF63xao58xh9FXYJ',
        'NMIXX': '28ot3wh4oNmoFOdVajibBl',
        'VIVIZ': '7Lq3yAtwi0Z7zpxEwbQQNZ',
        'LE SSERAFIM': '4SpbR6yFEvexJuaBpgAU5p',
        'New Jeans': '6HvZYsbFfjnjFrWF950C9d'
    }

    def __init__(self, base_url, artist_name, artist_id, country_code, client_id, client_secret):
        self.base_url = base_url
        self.artist_name = artist_name
        self.artist_id = artist_id
        self.country_code = country_code
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = ''
        self.kpop_girl_group_track_info = []


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


    async def get_track_info_from_artist_top_tracks(self, session):
        artist_top_tracks_url = f'{self.base_url}/artists/{self.artist_id}/top-tracks'

        params = {
            'market': self.country_code
        }

        result = await self.fetch_data_from_api(
            session,
            params,
            artist_top_tracks_url
        )

        track_info = []
        for track in result['tracks']:
            val = {
                'track_id': track['id'],
                'track_name': track['name'],
                'group_name': self.artist_name,
                'group_id': self.artist_id,
                'artists_name': [artist['name'] for artist in track['artists']],
                'album_release_date': track['album']['release_date'],
                'track_popularity': track['popularity'],
                'duration_ms': track['duration_ms']
            }
            track_info.append(val)

        self.kpop_girl_group_track_info.extend(track_info)


    async def main(self):
        await self.fetch_api_token()
        
        async with aiohttp.ClientSession() as session:
            await self.get_track_info_from_artist_top_tracks(session)

        return self.kpop_girl_group_track_info


    @staticmethod
    def save_json(file_name, results):
        file_path = f'./static/{file_name}.json'
        result = {'results': results}

        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(result, json_file, ensure_ascii=False, indent=4)
        
        return file_path
    

    @staticmethod
    def upload_to_blob(file_path, blob_file_name, account_name, account_key, container_name):
        today = date.today()
        year = str(today.year)
        month = str(today.month).zfill(2)
        day = str(today.day).zfill(2)

        blob_path = f'group/year={year}/month={month}/day={day}/{blob_file_name}.json'

        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key
        )

        container_client = blob_service_client.get_container_client(container_name)

        with open(file_path, 'rb') as data:
            container_client.upload_blob(name=blob_path, data=data)

        print(f'End Upload to {container_name} container {blob_path} blob.')
