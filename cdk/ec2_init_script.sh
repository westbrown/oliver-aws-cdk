#!/bin/bash
sudo yum update -y
sudo yum install git -y
# install apache server
sudo yum install httpd.x86_64 -y 
sudo systemctl enable httpd.service
sudo systemctl start httpd.service
# install kinesis-agent
sudo yum install aws-kinesis-agent -y
# reconfig kinesis-agent.config
sudo systemctl start aws-kinesis-agent