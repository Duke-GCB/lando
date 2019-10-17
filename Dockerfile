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
RUN curl https://get.helm.sh/helm-v3.0.0-beta.4-linux-amd64.tar.gz | tar xvz && chmod a+x linux-amd64/helm
CMD lando
