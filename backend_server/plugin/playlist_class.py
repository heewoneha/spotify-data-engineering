from plugin.base_class import BaseScraper
import aiohttp
import asyncio


class PlaylistScraper(BaseScraper):
    def __init__(self, base_url, playlist_uri, client_id, client_secret):
        super().__init__(base_url, client_id, client_secret)
        self.playlist_uri = playlist_uri
        self.track_audio_features = []


    async def get_track_lists_from_playlist(self, session):
        top50_playlist_url = f'{self.base_url}/playlists/{self.playlist_uri}/tracks'

        track_ids = []
        result = await self.fetch_data_from_api(
            session=session,
            params=None,
            api_url=top50_playlist_url
        )
        track_ids.extend([track['track']['id'] for track in result['items']])

        track_info = []
        index = 0
        for track in result['items']:
            index += 1
            val = {
                'track_id': track['track']['id'],
                'track_name': track['track']['name'],
                'artists_name': [artist['name'] for artist in track['track']['artists']],
                'album_release_date': track['track']['album']['release_date'],
                'track_number_in_playlist': index
            }
            track_info.append(val)

        return track_ids, track_info


    async def get_audio_features_from_track(self, session, track_id):
        track_info_url = f'{self.base_url}/audio-features/{track_id}'
        
        result = await self.fetch_data_from_api(
            session=session,
            params=None,
            api_url=track_info_url
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
            track_id_list, track_info = await self.get_track_lists_from_playlist(session)
            tasks = [self.get_audio_features_from_track(session, track_id) for track_id in track_id_list]
            await asyncio.gather(*tasks)

        return track_info, self.track_audio_features
