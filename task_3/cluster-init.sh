#!/bin/bash

retries_shards_add=30

set -e

echo "Waiting for config server..."

until mongosh --host mg_db_1:27017 --quiet --eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null | grep 1 >/dev/null; do
  sleep 2
done

echo "Initializing config replica set (if needed)..."

mongosh --host mg_db_1:27017 --quiet <<EOF
try {
  rs.status()
  print("Config RS already initialized")
} catch (e) {
  rs.initiate({
    _id: "cfgrs",
    configsvr: true,
    members: [
      { _id: 0, host: "mg_db_1:27017" },
      { _id: 1, host: "mg_db_2:27017" },
      { _id: 2, host: "mg_db_3:27017" }
    ]
  })
}
EOF

echo "Waiting for config PRIMARY..."

until mongosh --host mg_db_1:27017 --quiet --eval "db.hello().isWritablePrimary" | grep true >/dev/null; do
  sleep 2
done


echo "Waiting for shard server..."

until mongosh --host shard1s1:27017 --quiet --eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null | grep 1 >/dev/null; do
  sleep 2
done

echo "Initializing shard replica set (if needed)..."

mongosh --host shard1s1:27017 --quiet <<EOF
try {
  rs.status()
  print("Shard RS already initialized")
} catch (e) {
  rs.initiate({
    _id: "shard1rs",
    members: [
      { _id: 0, host: "shard1s1:27017" },
      { _id: 1, host: "shard1s2:27017" },
      { _id: 2, host: "shard1s3:27017" }
    ]
  })
}
EOF

echo "Waiting for shard PRIMARY..."

until mongosh --host shard1s1:27017 --quiet --eval "db.hello().isWritablePrimary" | grep true >/dev/null; do
  sleep 2
done


echo "Waiting for mongos..."

until mongosh --host mongos:27017 --quiet --eval "db.runCommand({ ping: 1 }).ok" 2>/dev/null | grep 1 >/dev/null; do
  sleep 2
done

echo "Adding shard (retry with limit)..."

until mongosh --host mongos:27017 --quiet <<EOF
try {
  const shards = sh.status().shards.map(s => s._id)
  if (!shards.includes("shard1rs")) {
    sh.addShard("shard1rs/shard1s1:27017,shard1s2:27017,shard1s3:27017")
    print("Shard added")
  } else {
    print("Shard already exists")
  }
} catch (e) {
  throw e
}
EOF
do
  retries_shards_add=$((retries_shards_add-1))

  if [ "$retries_shards_add" -le 0 ]; then
    echo "Failed to add shard after multiple attempts"
    exit 1
  fi

  echo "Retrying shard add... ($retries_shards_add left)"
  sleep 3
done

echo "Adding AIS database with sharding"

mongosh --host mongos:27017 --quiet <<EOF

// Switch to database
db = db.getSiblingDB("AIS_04_18")

// Create collection
db.createCollection("data")

// Create database
db.data.insertOne({
  initialized: true,
  timestamp: new Date()
})

// Enable sharding on database
sh.enableSharding("AIS_04_18")

// Shard the collection
sh.shardCollection("AIS_04_18.data", { timestamp: 1 })


print("AIS_04_18 sharding configured")

EOF

echo "Cluster setup complete!"