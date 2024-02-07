FROM python:3.10-slim 

RUN useradd -r -s /bin/bash spotify_scraper

ENV FastAPI_ENV=production

WORKDIR /app

COPY scraper_server/requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r ./requirements.txt

RUN if [ ! -d "/static" ]; then mkdir /static; fi

COPY scraper_server/. .

EXPOSE 8000

CMD ["python", "server.py"]
