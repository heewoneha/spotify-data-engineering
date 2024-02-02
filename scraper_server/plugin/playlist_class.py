from base64 import b64encode
from datetime import date
from azure.storage.blob import BlobServiceClient
import aiohttp
import asyncio
import json


class PlaylistScraper:
    def __init__(self, base_url, playlist_uri, client_id, client_secret):
        self.base_url = base_url
        self.playlist_uri = playlist_uri
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = ''
        self.track_audio_features = []


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


    async def fetch_data_from_api(self, session, api_url):
        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

        async with session.get(api_url, headers=headers) as response:
            if response.status != 200:
                print(f'Error occurred: {response.status} {response}')
                return {}
            response_text = await response.text(encoding='utf-8')
            data = json.loads(response_text)

            return data


    ### 가수명, 노래 제목, 발매일, track 순서(순위)는 추후에 추가 ###
    async def get_track_lists_from_top50_playlist(self, session):
        top50_playlist_url = f'{self.base_url}/playlists/{self.playlist_uri}/tracks'

        track_ids = []
        result = await self.fetch_data_from_api(
            session,
            top50_playlist_url
        )
        track_ids.extend([track['track']['id'] for track in result['items']])

        return track_ids
    

    async def get_track_audio_features(self, session, track_id):
        track_info_url = f'{self.base_url}/audio-features/{track_id}'
        
        result = await self.fetch_data_from_api(
            session,
            track_info_url
        )

        val = {
            'track_id': track_id,
            'acousticness': result['acousticness'],
            'danceability': result['danceability'],
            'duration_ms': result['duration_ms'],
            'energy': result['energy'],
            'instrumentalness': result['instrumentalness'],
            'key': result['key'],
            'liveness': result['liveness'],
            'loudness': result['loudness'],
            'mode': result['mode'],
            'speechiness': result['speechiness'],
            'tempo': result['tempo'],
            'time_signature': result['time_signature'],
            'valence': result['valence']
        }

        self.track_audio_features.append(val)
        await asyncio.sleep(2)
    
    
    async def main(self):
        await self.fetch_api_token()

        async with aiohttp.ClientSession() as session:
            track_id_lists = await self.get_track_lists_from_top50_playlist(session)
            tasks = [self.get_track_audio_features(session, track_id) for track_id in track_id_lists]
            await asyncio.gather(*tasks)

        return self.track_audio_features


    @staticmethod
    def save_json(total_top50_tracks):
        file_path = './static/top50.json'
        result = {'results': total_top50_tracks}

        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(result, json_file, ensure_ascii=False, indent=4)
        
        return file_path
    

    @staticmethod
    def upload_to_blob(file_path, account_name, account_key, container_name):
        today = date.today()
        year = str(today.year)
        month = str(today.month).zfill(2)
        day = str(today.day).zfill(2)

        blob_path = f'top50/year={year}/month={month}/day={day}/top50.json'

        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key
        )

        container_client = blob_service_client.get_container_client(container_name)

        with open(file_path, 'rb') as data:
            container_client.upload_blob(name=blob_path, data=data)

        print(f'End Upload to {container_name} container {blob_path} blob.')
