FROM bitnami/python:3.9-debian-12

ENV TZ=Europe/Kiev

WORKDIR /app

COPY tmp    /app/
COPY requirements.txt /app/
COPY creds/credentials.json /app/creds/credentials.json
COPY main.py /app/
COPY changes2.py /app/
COPY to_cloud.py /app/
COPY fbextract.py /app/

RUN apt-get update  
RUN apt-get install  libfbclient2 -y --no-install-recommends
RUN pip install -r requirements.txt

CMD [ "python3", "main.py" ]


