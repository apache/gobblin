#!/bin/bash
mkdir -p /home/ec2-user/cluster/1
yum install nfs-utils nfs-utils-lib
echo '/home/ec2-user/cluster *(rw,sync,no_subtree_check,fsid=1,no_root_squash)' | tee --append /etc/exports
/etc/init.d/nfs start
exportfs -a
mkdir -p /home/ec2-user/cluster/log-dir/
chown -R ec2-user:ec2-user /home/ec2-user/*
vr=0.7.1
cgS3=https://s3-us-west-2.amazonaws.com/some-bucket/cluster-conf/
cg=/home/ec2-user/cluster/cluster-conf/
jrS3=https://s3-us-west-2.amazonaws.com/some-bucket/gobblin-jars/
jr=/home/ec2-user/cluster/gobblin-jars/
wget -P "${cg}" "${cgS3}"application.conf
wget -P "${cg}" "${cgS3}"log4j-aws.properties
wget -P "${cg}" "${cgS3}"quartz.properties
wget -P "${jr}" "${jrS3}"myjar1.jar
wget -P "${jr}" "${jrS3}"myjar2.jar
wget -P "${jr}" "${jrS3}"myjar3.jar
wget -P "${jr}" "${jrS3}"myjar4-"${vr}".jar
java -cp /home/ec2-user/cluster/cluster-conf/:/home/ec2-user/cluster/gobblin-jars/* -Xmx-Xms1G  org.apache.gobblin.aws.GobblinAWSClusterManager --app_name cluster --gobblin.aws.work.dir /home/ec2-user/cluster/work-dir/ 1>/home/ec2-user/cluster/log-dir/GobblinAWSClusterManager.master.stdout 2>/home/ec2-user/cluster/log-dir/GobblinAWSClusterManager.master.stderr
