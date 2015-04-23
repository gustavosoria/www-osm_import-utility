#!/usr/bin/env bash

if [ -z "$1" ]
  then
    echo "No argument supplied. Please select the source path"
else {
	osm2pgsql -d gis_temp -l $1 && \
	node start-import && \
	node clean-temp-database && \
	echo "done"
}
fi	
