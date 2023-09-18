FROM python:3.8-buster

ARG GIT_BRANCH="Audiom"
#Change to HEAD in the future once normalizer is based on schema
ARG GIT_HASH="0e7ba02911b2c7937096317a895c121639d7ed02"

RUN apt-get update && apt-get install -y osmium-tool osmosis

RUN mkdir /app
WORKDIR /app
COPY . /app
RUN pip install -r /app/requirements.txt
RUN pip install .

RUN git clone -n --branch $GIT_BRANCH https://github.com/OpenSidewalks/OpenSidewalks-Schema.git /schema
WORKDIR /schema
RUN git checkout $GIT_HASH opensidewalks.schema.json

ENTRYPOINT osw_data
