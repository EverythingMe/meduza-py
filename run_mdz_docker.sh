#!/bin/sh
exec evme run docker.doit9.com/meduza/run --volume config ./test --entry-point testmain -- -confdir=/config $@
