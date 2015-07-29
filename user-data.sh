#!/usr/bin/env bash
# the contents of this file should be put in the user data field
sudo apt-get update
sudo apt-get install -y git python-dev python-pip
cd /home/ubuntu/
sudo git clone https://github.com/executivereader/mongo-startup.git
sudo git clone https://github.com/executivereader/article-mapper.git
sudo cp /home/ubuntu/mongo-startup/connection_string.txt /home/ubuntu/article-mapper/connection_string.txt
sudo cp /home/ubuntu/mongo-startup/update_replica_set.py /home/ubuntu/article-mapper/update_replica_set.py
cd /home/ubuntu/article-mapper
sudo pip install pymongo
sudo screen -dm bash -c "sudo python article-mapper.py"
