FROM python:3

#RUN apk update
#RUN apk add musl-dev wget git build-base libffi-dev openssl-dev python3-dev

# Numpy
#RUN pip install cython
#RUN ln -s /usr/include/locale.h /usr/include/xlocale.h

WORKDIR /app
COPY . /app

# TA-Lib
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
  tar -xvzf ta-lib-0.4.0-src.tar.gz && \
  cd ta-lib/ && \
  ./configure --prefix=/usr && \
  make && \
  make install

RUN pip install . && \
    pip install git+https://github.com/imvinaypatil/webull.git@slave -U

RUN cd ta-lib/ && make uninstall
#RUN apk del musl-dev wget git build-base libffi-dev openssl-dev python3-dev

ENV PYTHONUNBUFFERED=1
ENV dbport=27017
ENV orderbook=true
ENV threads=2
ENV dbhost='127.0.0.1'
ENV dbport=27017
ENV dbuser=qtpy
ENV dbpassword=qtpy
ENV dbname=qtpy
ENV dbskip=false
ENV LOGLEVEL=INFO

CMD [ "python", "-m", "kinetick.factory.blotter" ]

