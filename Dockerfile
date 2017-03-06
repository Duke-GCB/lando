# Runs master branch from github of lando/lando-messaging
# Requires a lando config file mounted at /etc/lando_config.yml.
FROM python:2.7.13
RUN pip install git+git://github.com/Duke-GCB/lando-messaging.git
RUN pip install git+git://github.com/Duke-GCB/lando.git
ENV WORKDIR /work
WORKDIR ${WORKDIR}
CMD lando
