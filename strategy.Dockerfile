FROM python:3.7

WORKDIR /app
ADD ./requirements.txt /app/requirements.txt
ADD ./constraints.txt /app/constraints.txt

RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
  tar -xvzf ta-lib-0.4.0-src.tar.gz && \
  cd ta-lib/ && \
  ./configure --prefix=/usr && \
  make && \
  make install


RUN pip install -r requirements.txt -c constraints.txt && \
    pip install git+https://github.com/imvinaypatil/webull.git@slave -U

ADD . /app

ENV dbport=27017
ENV orderbook=true
ENV threads=4
ENV dbhost='127.0.0.1'
ENV dbport='27017'
ENV dbuser=kinetick
ENV dbpassword=kinetick
ENV dbname=kinetick
ENV dbskip=false
ENV zerodha_user=kinetick
ENV zerodha_password=kinetick
ENV zerodha_pin=kinetick
ENV resolution=1m
ENV LOGLEVEL=INFO
ENV PYTHONUNBUFFERED=1

CMD [ "python", "-m", "kinetick.factory.strategy" ]
