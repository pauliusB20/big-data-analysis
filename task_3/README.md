Task 3 - Noise filtering

Project structure:

* main.py - main code that runs parallel data insert 

* filter.py - code for filtering out noise in dataset

* time_analysis.py - code for creating time delta histograms

* docker-compose.yml - docker stack config file that has all the defined 
services to run the docker mongo db cluster

* mongo-entrypoint.sh and cluster-init.sh - shell scripts for configuring
mongo db containers


In order to start docker cluster

docker-compose up -d

NOTE: 
Install docker compose and enable docker.service