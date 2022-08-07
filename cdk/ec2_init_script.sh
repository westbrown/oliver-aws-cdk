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
sudo -i 
echo '{
  "cloudwatch.emitMetrics": true, 
  "firehose.endpoint": "firehose.us-east-1.amazonaws.com",
  
  "flows": [
    {
      "filePattern": "/var/log/httpd/access_log*",
      "deliveryStream": "web-log-ingestion-stream",
      "dataProcessingOptions": [
        {
          "optionName": "LOGTOJSON",
          "logFormat": "COMMONAPACHELOG"
      }]
    }
  ]
}' > /etc/aws-kinesis/agent.json
chown -R  aws-kinesis-agent-user:aws-kinesis-agent-user /var/log/httpd
systemctl start aws-kinesis-agent
