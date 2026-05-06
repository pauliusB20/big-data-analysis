#!/bin/sh
set -e

echo "Waiting for config servers to start..."
until mongosh --host mongo_1_config:27017 --quiet --eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null; do
  sleep 2
done

echo "Initializing config replica set..."
mongosh --host mongo_1_config:27017 --quiet <<EOF
rs.initiate({
  _id: "cfgrs",
  configsvr: true,
  members: [
    { _id: 0, host: "mongo_1_config:27017" },
    { _id: 1, host: "mongo_2_config:27017" },
    { _id: 2, host: "mongo_3_config:27017" }
  ]
})
EOF

echo "Waiting for cfgrs PRIMARY..."

until mongosh --host mongo_1_config:27017 --quiet --eval \
'rs.status().myState == 1' 2>/dev/null; do
  sleep 2
done

echo "Waiting for shard servers to start..."
until mongosh --host shard1s1:27017 --quiet --eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null; do
  sleep 2
done

until mongosh --host shard2s1:27017 --quiet --eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null; do
  sleep 2
done

until mongosh --host shard3s1:27017 --quiet --eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null; do
  sleep 2
done

echo "Initializing shard shard1s replica set..."
mongosh --host shard1s1:27017 --quiet <<EOF
rs.initiate({
  _id: "shard1rs",
  members: [
    { _id: 0, host: "shard1s1:27017" },
    { _id: 1, host: "shard1s2:27017" },
    { _id: 2, host: "shard1s3:27017" }
  ]
})
EOF

echo "Waiting for shard1rs PRIMARY..."

until mongosh --host shard1s1:27017 --quiet --eval \
'rs.status().myState == 1' 2>/dev/null; do
  sleep 2
done

echo "Initializing shard shard2rs replica set..."
mongosh --host shard2s1:27017 --quiet <<EOF
rs.initiate({
  _id: "shard2rs",
  members: [
    { _id: 0, host: "shard2s1:27017" },
    { _id: 1, host: "shard2s2:27017" },
    { _id: 2, host: "shard2s3:27017" }
  ]
})
EOF

echo "Waiting for shard2rs PRIMARY..."

until mongosh --host shard2s1:27017 --quiet --eval \
'rs.status().myState == 1' 2>/dev/null; do
  sleep 2
done

echo "Initializing shard shard3s replica set..."
mongosh --host shard3s1:27017 --quiet <<EOF
rs.initiate({
  _id: "shard3rs",
  members: [
    { _id: 0, host: "shard3s1:27017" },
    { _id: 1, host: "shard3s2:27017" },
    { _id: 2, host: "shard3s3:27017" }
  ]
})
EOF

echo "Waiting for shard3rs PRIMARY..."

until mongosh --host shard3s1:27017 --quiet --eval \
'rs.status().myState == 1' 2>/dev/null; do
  sleep 2
done

echo "Waiting for mongos to be ready..."
until mongosh --host mongos:27017 --quiet --eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null; do
  sleep 2
done

echo "Adding Shard to Cluster..."
# Simplified: attempt to add shard; catch error if already exists
mongosh --host mongos:27017 --quiet <<EOF
try {
  sh.addShard("shard1rs/shard1s1:27017,shard1s2:27017,shard1s3:27017")
  sh.addShard("shard2rs/shard2s1:27017,shard2s2:27017,shard2s3:27017")
  sh.addShard("shard3rs/shard3s1:27017,shard3s2:27017,shard3s3:27017")
  print("Shard added successfully")
} catch (e) {
  print("Shard already present or error: " + e.message)
}
EOF

echo "Configuring Sharding for AIS_04_18..."
mongosh --host mongos:27017 --quiet <<EOF
sh.enableSharding("AIS_04_18")
db = db.getSiblingDB("AIS_04_18")
sh.shardCollection("AIS_04_18.data", { mmsi: "hashed" })
db.data.createIndex({ timestamp: 1 })
print("AIS_04_18 sharding configured!")
EOF

echo "Cluster setup complete!"