FROM python:3.9-slim
ENV TZ=Europe/Kiev

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends libfbclient2 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir tmp
COPY requirements.txt /app/
COPY creds/credentials.json /app/creds/credentials.json
COPY *.py /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python3", "main.py" ]