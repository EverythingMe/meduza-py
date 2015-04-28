#!/bin/bash
docker stop docker.doit9.com_meduza_run || echo 'Not running'
CONFIG_VOLUME=`dirname ${@: -1}`
CONFIG_FILE=/config/`basename ${@: -1}`
LENGTH=$(($#-1))
exec evme run docker.doit9.com/meduza/run --no-tty --volume config $CONFIG_VOLUME --entry-point testmain -- ${@: 1:$LENGTH} $CONFIG_FILE
