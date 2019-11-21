#!/bin/bash
# the time variable is the amount of seconds from 1970-01-01 00:00:00 UTC
time=$(date +%s)

# Downloading the osm file
curl https://api.openstreetmap.org/api/0.6/map?bbox=10.6557,59.4089,10.7023,59.4302 --output map$time.osm

# Uploads the datafile
hdfs dfs -put map$time.osm  /xmlhadoop/map$time.osm
