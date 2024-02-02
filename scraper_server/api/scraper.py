from fastapi import APIRouter, HTTPException
from fastapi.staticfiles import StaticFiles
from plugin.playlist_class import PlaylistScraper
import os
import time


router = APIRouter(prefix='/api/v1')

path = 'static'
isExist = os.path.exists(path)
if not isExist:
    os.makedirs(path)
router.mount('/static', StaticFiles(directory='static'), name='static')


@router.get('/scrape-top50-playlist')
async def top50_playlist_scrape_jobs():
    try:
        start_time = time.time()
        
        base_url =  'https://api.spotify.com/v1'
        playlist_uri = '37i9dQZEVXbNxXF4SkHj9F'
        client_id = os.getenv('SPOTIFY_CLIENT_ID')
        client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')

        scraper = PlaylistScraper(
            base_url=base_url,
            playlist_uri=playlist_uri,
            client_id=client_id,
            client_secret=client_secret
        )
        result = await scraper.main()

        azure_blob_account_name = os.getenv('AZURE_BLOB_ACCOUNT_NAME')
        azure_blob_account_key = os.getenv('AZURE_BLOB_ACCOUNT_KEY')
        azure_blob_container_name = os.getenv('AZURE_BLOB_CONTAINER_NAME')

        file_path = PlaylistScraper.save_json(result)
        PlaylistScraper.upload_to_blob(
            file_path=file_path,
            account_name=azure_blob_account_name,
            account_key=azure_blob_account_key,
            container_name=azure_blob_container_name
        )

        end_time = time.time()
        scraped_time = end_time - start_time
        
        print(f'took {scraped_time} seconds to scrape {len(result)} tracks and upload to Blob')

        return {"message": f"Scraped {len(result)} tracks and uploaded to Blob successfully!"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
