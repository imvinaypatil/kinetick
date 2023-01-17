FROM python:3.7-slim

WORKDIR /app
ADD ./requirements.txt /app/requirements.txt
ADD ./constraints.txt /app/constraints.txt

RUN apt-get update && apt-get install -y wget && apt-get install -y build-essential && apt-get install -y git

RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
  tar -xvzf ta-lib-0.4.0-src.tar.gz && \
  cd ta-lib/ && \
  ./configure --prefix=/usr --build=aarch64-unknown-linux-gnu && \
  make && \
  make install

RUN python -m pip install --upgrade pip &&  \
    pip install -r requirements.txt -c constraints.txt --no-cache-dir

RUN apt-get autoremove --purge --yes build-essential

ADD . /app

ENV dbport=27017
ENV orderbook=true
ENV threads=4
ENV dbhost='127.0.0.1'
ENV dbport='27017'
ENV dbuser=kinetick
ENV dbpassword=kinetick
ENV dbname=kinetick
ENV dbskip=true
ENV zerodha_user=kinetick
ENV zerodha_password=kinetick
ENV zerodha_pin=kinetick
ENV resolution=1m
ENV LOGLEVEL=INFO
ENV PYTHONUNBUFFERED=1

CMD [ "python", "-m", "kinetick.factory.strategy" ]
