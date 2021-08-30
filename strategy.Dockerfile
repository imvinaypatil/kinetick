FROM python:3

#RUN apk update
#RUN apk add musl-dev wget git build-base libffi-dev openssl-dev python3-dev

# Numpy
#RUN pip install cython
#RUN ln -s /usr/include/locale.h /usr/include/xlocale.h

# TA-Lib
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
  tar -xvzf ta-lib-0.4.0-src.tar.gz && \
  cd ta-lib/ && \
  ./configure --prefix=/usr && \
  make && \
  make install

WORKDIR /app
COPY . /app

RUN pip3 install . && \
    pip3 install git+https://github.com/imvinaypatil/webull.git@slave -U

#RUN apk del musl-dev wget git build-base libffi-dev openssl-dev python3-dev

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
