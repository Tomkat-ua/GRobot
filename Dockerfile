FROM bitnami/python:3.9-debian-12

ENV TZ=Europe/Kiev

WORKDIR /app

COPY requirements.txt /app/
COPY creds/credentials.json /app/
COPY *.py /app/

RUN apt-get update  
RUN apt-get install  libfbclient2 -y --no-install-recommends
RUN pip install -r requirements.txt

CMD [ "python3", "main.py" ]


