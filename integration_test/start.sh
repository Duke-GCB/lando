#!/usr/bin/env bash
LANDO_USERNAME='lando'
LANDO_PASSWORD='secret'

echo "Launching rabbitmq."
docker run -d --name lando_rabbit -p 5672:5672 -p 15672:15672 -e RABBITMQ_NODENAME=my-rabbit -e RABBITMQ_DEFAULT_USER=$LANDO_USERNAME -e RABBITMQ_DEFAULT_PASS=$LANDO_PASSWORD rabbitmq:latest

echo "Launching bespin-api."
cd bespin-api
source env/bin/activate
python manage.py runserver
deactivate

echo "Stopping rabbitmq."
docker rm --force lando_rabbit 
