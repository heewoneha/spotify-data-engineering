from plugin.base_class import BaseScraper
import aiohttp


class ArtistTrackScraper(BaseScraper):
    kpop_boy_group_category_dict = {
        'BTOB': '2hcsKca6hCfFMwwdbFvenJ',
        'EXO': '3cjEqqelV9zb4BYE3qDQ4O',
        'BTS': '3Nrfpe0tUJi4K4DXYWgMUX',
        'WINNER': '5DuzBeOgFwViFcv00Q5PFb',
        'JANNABI': '2SY6OktZyMLdOnscX3DCyS',
        'MONSTA X': '4TnGh5PKbSjpYqpIdlW5nz',
        'N.Flying': '2ZmXexIJAD7PgABrj0qQRb',
        'SEVENTEEN': '7nqOGRxlXj7N2JYbgNEjYH',
        'DAY6': '5TnQc2N1iKlFjYD7CPGvFc',
        'MeloMance': '6k4r73Wq8nhkCDoUsECL1e',
        'NCT': '48eO052eSDcn8aTxiv6QaG',
        'NCT 127': '7f4ignuCJhLXfZ9giKT7rH',
        'NCT U': '3paGCCtX1Xr4Gx53mSeZuQ',
        'NCT DREAM': '1gBUSTR3TyDdTVFIaQnc02',
        'NCT DOJAEJUNG': '0W0w607z3JEA1vXLz9FVGw',
        'SF9': '7LOmc7gyMVMOWF8qwEdn2X',
        'THE BOYZ': '0CmvFWTX9zmMNCUi6fHtAx',
        'Stray Kids': '2dIgFjalVxs4ThymZ67YCE',
        'ATEEZ': '68KmkJeZGfwe1OUaivBa2L',
        'TOMORROW X TOGETHER': '0ghlgldX5Dd6720Q3qFyQB',
        'TREASURE': '3KonOYiLsU53m4yT7gNotP',
        'ENHYPEN': '5t5FqBwTcgKTaWmfEbwQY9',
        'ZEROBASEONE': '7cjg7EkeZy3OI5o9Qthc6n',
        'RIIZE': '2jOm3cYujQx6o1dxuiuqaX',
        'TWS': '4GgBKgxhc649frZDHcXIEz'
    }

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
        'NewJeans': '6HvZYsbFfjnjFrWF950C9d',
        'BABYMONSTER': '1SIocsqdEefUTE6XKGUiVS'
    }


    def __init__(self, base_url, artist_name, artist_id, country_code, client_id, client_secret):
        super().__init__(base_url, client_id, client_secret)
        self.artist_name = artist_name
        self.artist_id = artist_id
        self.country_code = country_code
        self.group_artist_info = []
        self.group_track_info = []


    async def get_artist_info(self, session):
        artist_info_url = f'{self.base_url}/artists/{self.artist_id}'

        result = await self.fetch_data_from_api(
            session=session,
            params=None,
            api_url=artist_info_url
        )

        val = {
            'group_id': self.artist_id,
            'group_name': self.artist_name,
            'followers': result['followers']['total'],
            'group_genres': result['genres'],
            'group_popularity': result['popularity']
        }

        self.group_artist_info.append(val)


    async def get_track_info_from_artist_top_tracks(self, session):
        artist_top_tracks_url = f'{self.base_url}/artists/{self.artist_id}/top-tracks'

        params = {
            'market': self.country_code
        }

        result = await self.fetch_data_from_api(
            session=session,
            params=params,
            api_url=artist_top_tracks_url
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

        self.group_track_info.extend(track_info)


    async def main(self):
        await self.fetch_api_token()
        
        async with aiohttp.ClientSession() as session:
            await self.get_artist_info(session)
            await self.get_track_info_from_artist_top_tracks(session)

        return self.group_artist_info, self.group_track_info
