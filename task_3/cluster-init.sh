#!/bin/sh
set -e

echo "Waiting for config servers to start..."
until mongosh --host mongo_1_config:27017 --quiet --eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null; do
  sleep 2
done

echo "Initializing config replica set..."
mongosh --host mongo_1_config:27017 --quiet --eval '
try {
  rs.initiate({
    _id: "cfgrs",
    configsvr: true,
    members: [
      { _id: 0, host: "mongo_1_config:27017" },
      { _id: 1, host: "mongo_2_config:27017" },
      { _id: 2, host: "mongo_3_config:27017" }
    ]
  })
} catch (e) {
  printjson(e)
}
'

echo "Waiting for cfgrs PRIMARY..."

# until mongosh --host mongo_1_config:27017 --quiet --eval \
# 'rs.status().members.some(m => m.stateStr == "PRIMARY")' \
# 2>/dev/null | grep -q true; do
#   sleep 2
# done

until mongosh --host mongo_1_config:27017 --quiet --eval \
'rs.status().members.some(m => m.stateStr == "PRIMARY")' \
| grep -q true; do
  sleep 2
done

echo "Waiting for shard servers to start..."
until mongosh --host shard1s1:27017 --quiet --eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null; do
  sleep 2
done

until mongosh --host shard2s1:27017 --quiet --eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null; do
  sleep 2
done


echo "Initializing shard shard1s replica set..."
mongosh --host shard1s1:27017 --quiet <<EOF
try {
  print("shard1rs already initialized")
  rs.initiate({
    _id: "shard1rs",
    members: [
      { _id: 0, host: "shard1s1:27017" },
      { _id: 1, host: "shard1s2:27017" },
      { _id: 2, host: "shard1s3:27017" }
    ]
  })
} catch (e) {
  printjson(e)
}
EOF

echo "Waiting for shard1rs PRIMARY..."

until mongosh --host shard1s1:27017 --quiet --eval \
'rs.status().members.some(m => m.stateStr == "PRIMARY")' \
2>/dev/null | grep -q true; do
  sleep 2
done

echo "Initializing shard shard2rs replica set..."
mongosh --host shard2s1:27017 --quiet <<EOF
try {
   rs.initiate({
    _id: "shard2rs",
    members: [
      { _id: 0, host: "shard2s1:27017" },
      { _id: 1, host: "shard2s2:27017" },
      { _id: 2, host: "shard2s3:27017" }
    ]
  })
  print("shard2rs already initialized")
} catch (e) {
   printjson(e)
}
EOF

echo "Waiting for shard2rs PRIMARY..."

until mongosh --host shard2s1:27017 --quiet --eval \
'rs.status().members.some(m => m.stateStr == "PRIMARY")' \
2>/dev/null | grep -q true; do
  sleep 2
done

# NOTE: Keep repeating untill the command succeeds. If fails, sleeps 2 seconds

echo "Waiting for mongos to be ready..."
until mongosh --host mongos:27017 --quiet --eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null; do
  sleep 2
done

echo "Adding Shard to Cluster..."
# Simplified: attempt to add shard; catch error if already exists
mongosh --host mongos:27017 --quiet <<EOF
for (const shard of [
  "shard1rs/shard1s1:27017,shard1s2:27017,shard1s3:27017",
  "shard2rs/shard2s1:27017,shard2s2:27017,shard2s3:27017",
]) {
  const result = sh.addShard(shard)
  printjson(result)
}
EOF

echo "Configuring Sharding for AIS_04_18..."
mongosh --host mongos:27017 --quiet <<EOF
try {
  sh.enableSharding("AIS_04_18")
  print("Database sharding enabled")
} catch (e) {
  print("Database already sharded or error: " + e.message)
}

db = db.getSiblingDB("AIS_04_18")

try {
  sh.shardCollection(
    "AIS_04_18.data",
    { mmsi: "hashed" }
  )
  print("Collection sharded")
} catch (e) {
  print("Collection already sharded or error: " + e.message)
}

try {
  db.data.createIndex({ timestamp: 1 })
  print("Index created")
} catch (e) {
  print("Index already exists or error: " + e.message)
}
EOF

echo "Cluster setup complete!"