#!/bin/sh
set -e

echo "Waiting for config servers to start..."
until mongosh --host mg_db_1:27017 --quiet --eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null; do
  sleep 2
done

echo "Initializing config replica set..."
mongosh --host mg_db_1:27017 --quiet <<EOF
rs.initiate({
  _id: "cfgrs",
  configsvr: true,
  members: [
    { _id: 0, host: "mg_db_1:27017" },
    { _id: 1, host: "mg_db_2:27017" },
    { _id: 2, host: "mg_db_3:27017" }
  ]
})
EOF

echo "Waiting for shard servers to start..."
until mongosh --host shard1s1:27017 --quiet --eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null; do
  sleep 2
done

echo "Initializing shard replica set..."
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

echo "Waiting for mongos to be ready..."
until mongosh --host mongos:27017 --quiet --eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null; do
  sleep 2
done

echo "Adding Shard to Cluster..."
# Simplified: attempt to add shard; catch error if already exists
mongosh --host mongos:27017 --quiet <<EOF
try {
  sh.addShard("shard1rs/shard1s1:27017,shard1s2:27017,shard1s3:27017")
  print("Shard added successfully")
} catch (e) {
  print("Shard already present or error: " + e.message)
}
EOF

echo "Configuring Sharding for AIS_04_18..."
mongosh --host mongos:27017 --quiet <<EOF
sh.enableSharding("AIS_04_18")
db = db.getSiblingDB("AIS_04_18")
db.createCollection("data")
sh.shardCollection("AIS_04_18.data", { timestamp: 1 })
print("AIS_04_18 sharding configured!")
EOF

echo "Cluster setup complete!"