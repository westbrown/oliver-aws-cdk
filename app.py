#!/usr/bin/env python3

import aws_cdk as cdk

from cdk.cdk_stack import CdkStack
from cdk.log_processing_stack import LogProcessStack


app = cdk.App()
LogProcessStack(app, "ec2-web-server")
app.synth()
