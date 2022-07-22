#!/usr/bin/env sh
docker inspect consul-server{1,2,3} | jq -r .[].NetworkSettings.Networks[].IPAddress
