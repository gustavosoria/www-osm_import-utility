#!/usr/bin/env bash

if [ -z "$1" ]
  then
    echo "No argument supplied. Please select the source path"
else {
    echo "START: $(date)" && \
	osm2pgsql -d gis -l $1 && \
	node start-import && \
	node clean-temp-database && \
	echo "done" && \
	echo "END: $(date)"
}
fi	
