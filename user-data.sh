#!/usr/bin/env bash
# the contents of this file should be put in the user data field
sudo apt-get install -y git
cd /home/ubuntu/
sudo git clone https://github.com/executivereader/mongo-startup.git
sudo git clone https://github.com/executivereader/article-mapper.git
sudo cp /home/ubuntu/mongo-startup/connection_string.txt /home/ubuntu/article-mapper/connection_string.txt
cd /home/ubuntu/article-mapper
sudo python article_mapper.py
