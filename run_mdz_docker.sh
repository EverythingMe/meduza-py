#!/bin/sh
docker stop docker.doit9.com_meduza_run || echo 'Not running'
exec evme run docker.doit9.com/meduza/run --no-tty --volume config ./test --entry-point testmain -- -confdir=/config $@
