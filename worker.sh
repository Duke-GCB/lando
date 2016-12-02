#!/bin/bash
cd /lando
HOSTNAME=`hostname`
echo $HOSTNAME
python lando_worker.py $HOSTNAME
