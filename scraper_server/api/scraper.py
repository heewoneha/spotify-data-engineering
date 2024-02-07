from fastapi import APIRouter, HTTPException
from fastapi.staticfiles import StaticFiles
from plugin.playlist_class import PlaylistScraper
from plugin.artist_track_class import ArtistTrackScraper
import os
import asyncio
import time


router = APIRouter(prefix='/api/v1')

path = 'static'
isExist = os.path.exists(path)
if not isExist:
    os.makedirs(path)
router.mount('/static', StaticFiles(directory='static'), name='static')

base_url = 'https://api.spotify.com/v1'
client_id = os.getenv('SPOTIFY_CLIENT_ID')
client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')


@router.get('/scrape-top50-playlist')
async def top50_playlist_scrape_jobs():
    try:
        start_time = time.time()
        
        playlist_uri = '37i9dQZEVXbNxXF4SkHj9F'

        scraper = PlaylistScraper(
            base_url=base_url,
            playlist_uri=playlist_uri,
            client_id=client_id,
            client_secret=client_secret
        )
        track_info, track_audio_features = await scraper.main()

        azure_blob_account_name = os.getenv('AZURE_BLOB_ACCOUNT_NAME')
        azure_blob_account_key = os.getenv('AZURE_BLOB_ACCOUNT_KEY')
        azure_blob_container_name = os.getenv('AZURE_BLOB_CONTAINER_NAME')

        file_path = PlaylistScraper.save_json('top50_tracks_info', track_info)
        PlaylistScraper.upload_to_blob(
            file_path=file_path,
            blob_file_name='tracks_info',
            account_name=azure_blob_account_name,
            account_key=azure_blob_account_key,
            container_name=azure_blob_container_name
        )

        file_path = PlaylistScraper.save_json('top50_audio_features', track_audio_features)
        PlaylistScraper.upload_to_blob(
            file_path=file_path,
            blob_file_name='audio_features',
            account_name=azure_blob_account_name,
            account_key=azure_blob_account_key,
            container_name=azure_blob_container_name
        )

        end_time = time.time()
        scraped_time = end_time - start_time
        
        print(f'took {scraped_time} seconds to scrape {len(track_audio_features)} tracks and upload to Blob')

        return {"message": f"Scraped {len(track_audio_features)} tracks and uploaded to Blob successfully!"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/scrape-kpop-girl-group-tracks')
async def kpop_girl_group_tracks_scrape_jobs():
    try:
        start_time = time.time()

        tasks = []
        for artist_name, artist_id in ArtistTrackScraper.kpop_girl_group_category_dict.items():
            scraper = ArtistTrackScraper(
                base_url=base_url,
                artist_name=artist_name,
                artist_id=artist_id,
                country_code='KR',
                client_id=client_id,
                client_secret=client_secret
            )
            task = scraper.main()
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        artist_data_list, track_data_list = zip(*results)

        kpop_girl_group_artist_info = []
        for artist_sub_list in artist_data_list:
            kpop_girl_group_artist_info.extend(artist_sub_list)

        kpop_girl_group_track_info = []
        for track_sub_list in track_data_list:
            kpop_girl_group_track_info.extend(track_sub_list)

        azure_blob_account_name = os.getenv('AZURE_BLOB_ACCOUNT_NAME')
        azure_blob_account_key = os.getenv('AZURE_BLOB_ACCOUNT_KEY')
        azure_blob_container_name = os.getenv('AZURE_BLOB_CONTAINER_NAME')

        file_path = ArtistTrackScraper.save_json('kpop_girl_group_artist_info', kpop_girl_group_artist_info)
        ArtistTrackScraper.upload_to_blob(
            file_path=file_path,
            blob_file_name='kpop_girl_group_artist_info',
            account_name=azure_blob_account_name,
            account_key=azure_blob_account_key,
            container_name=azure_blob_container_name
        )

        file_path = ArtistTrackScraper.save_json('kpop_girl_group_track_info', kpop_girl_group_track_info)
        ArtistTrackScraper.upload_to_blob(
            file_path=file_path,
            blob_file_name='kpop_girl_group_track_info',
            account_name=azure_blob_account_name,
            account_key=azure_blob_account_key,
            container_name=azure_blob_container_name
        )

        end_time = time.time()
        scraped_time = end_time - start_time
        
        print(f'took {scraped_time} seconds to scrape {len(kpop_girl_group_artist_info)} artists & {len(kpop_girl_group_track_info)} tracks and upload to Blob')

        return {"message": f"Scraped {len(kpop_girl_group_artist_info)} artists & {len(kpop_girl_group_track_info)} tracks and uploaded to Blob successfully!"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
