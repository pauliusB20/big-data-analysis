#!/bin/sh
set -e

echo "Waiting for config servers..."

until mongosh --host mongo_1_config:27017 --quiet --eval \
"db.runCommand({ ping: 1 }).ok" 2>/dev/null; do
  sleep 2
done

until mongosh --host mongo_2_config:27017 --quiet --eval \
"db.runCommand({ ping: 1 }).ok" 2>/dev/null; do
  sleep 2
done

until mongosh --host mongo_3_config:27017 --quiet --eval \
"db.runCommand({ ping: 1 }).ok" 2>/dev/null; do
  sleep 2
done

echo "Waiting for cfgrs PRIMARY..."

until mongosh --host mongo_1_config:27017 --quiet --eval \
'rs.status().myState == 1' 2>/dev/null; do
  sleep 2
done

echo "Starting mongos..."

exec mongos \
  --configdb cfgrs/mongo_1_config:27017,mongo_2_config:27017,mongo_3_config:27017 \
  --bind_ip 0.0.0.0 \
  --port 27017