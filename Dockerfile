FROM python:latest

COPY . /app
WORKDIR /app
RUN apt-get update && apt-get install -y mariadb-client libmariadbclient-dev
RUN python setup.py build && python setup.py install

CMD voice-corpora-automation