Task 3 - Noise filtering

Project structure:

* main.py - main code that runs parallel data insert 

* filter.py - code for filtering out noise in dataset

* time_analysis.py - code for creating time delta histograms

* docker-compose.yml - docker YAML config file for creating 
mongo db sharded cluster

* mongo-entrypoint.sh and cluster-init.sh - shell scripts for configuring
mongo db containers to form the sharded cluster

NOTE: Linux OS is required for starting the cluster. Scripts for the cluster
written for Linux and not intended for cross platform (Like Windows or Mac).

In order to start docker cluster
1. Create python vietual environemtn using "python3 -m venv task_3_env". 
2. Activate the virtual environment using "source task_3_env/bin/activate"
3. Install packages with command "pip install --no-cache-dir -r requirements.txt"
4. Start docker service using "sudo systemctl start docker.service"
5. Initiate docker container build using "docker-compose up -d"
6. Wait 6 seconds and in the terminal type in "python3 main.py". 
Now the data will be inserted in the local mongodb, filtered and
histogram will be calculated


