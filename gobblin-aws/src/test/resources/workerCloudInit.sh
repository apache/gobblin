#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
mkdir -p /home/ec2-user/cluster
mount -t nfs4 0.0.0.0:/home/ec2-user/cluster /home/ec2-user/cluster
mkdir -p /home/ec2-user/cluster/log-dir/
chown -R ec2-user:ec2-user /home/ec2-user/*
vr=0.7.1
cg0=https://s3-us-west-2.amazonaws.com/some-bucket/cluster-conf/
cg=/home/ec2-user/cluster/cluster-conf/
jr0=https://s3-us-west-2.amazonaws.com/some-bucket/gobblin-jars/
jr=/home/ec2-user/cluster/gobblin-jars/
wget -P "${cg}" "${cg0}"application.conf
wget -P "${cg}" "${cg0}"log4j-aws.properties
wget -P "${cg}" "${cg0}"quartz.properties
wget -P "${jr}" "${jr0}"myjar1.jar
wget -P "${jr}" "${jr0}"myjar2.jar
wget -P "${jr}" "${jr0}"myjar3.jar
wget -P "${jr}" "${jr0}"myjar4-"${vr}".jar
pi=`curl http://169.254.169.254/latest/meta-data/local-ipv4`
java -cp /home/ec2-user/cluster/cluster-conf/:/home/ec2-user/cluster/gobblin-jars/* -Xmx-Xms1G  org.apache.gobblin.aws.GobblinAWSTaskRunner --app_name cluster --helix_instance_name $pi --gobblin.aws.work.dir /home/ec2-user/cluster/work-dir/ 1>/home/ec2-user/cluster/log-dir/GobblinAWSTaskRunner.$pi.stdout 2>/home/ec2-user/cluster/log-dir/GobblinAWSTaskRunner.$pi.stderr