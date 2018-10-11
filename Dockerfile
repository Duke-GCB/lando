# Runs master branch from github of lando/lando-messaging
# Requires a lando config file mounted at /etc/lando_config.yml.
FROM python:3.6
LABEL maintainer="john.bradley@duke.edu"

ADD . /src
WORKDIR /src
RUN python setup.py install
ENV WORKDIR /work
RUN mkdir ${WORKDIR}
WORKDIR ${WORKDIR}
CMD lando
