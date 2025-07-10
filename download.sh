#!/bin/bash

set -eu pipefail

wget -v --header="User-Agent: Mozilla/5.0 (Windows NT 6.0) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.97 Safari/537.11" --header="Referer: http://xmodulo.com/" --header="Accept-Encoding: compress, gzip" -O ./data/aircraft.zip https://registry.faa.gov/database/ReleasableAircraft.zip

cd ./data/

echo "Unzipping Aircraft data"

unzip aircraft.zip
