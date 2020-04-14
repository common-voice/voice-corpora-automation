FROM python:latest

COPY . /app
WORKDIR /app
RUN python setup.py build
RUN python setup.py install

CMD voice-corpora-automation