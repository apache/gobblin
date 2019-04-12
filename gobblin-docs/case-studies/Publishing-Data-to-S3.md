Table of Contents
--------------------

[TOC]

# Introduction

While Gobblin is not tied to any specific cloud provider, [Amazon Web Services](https://aws.amazon.com/) is a popular choice. This document will outline how Gobblin can publish data to [S3](https://aws.amazon.com/s3/). Specifically, it will provide a step by step guide to help setup Gobblin on Amazon [EC2](https://aws.amazon.com/ec2/), run Gobblin on EC2, and publish data from EC2 to S3.

It is recommended to configure Gobblin to first write data to [EBS](https://aws.amazon.com/ebs/), and then publish the data to S3. This is the recommended approach because there are a few caveats when working with with S3. See the [Hadoop and S3](#hadoop-and-s3) section for more details.

This document will also provide a step by step guide for launching and configuring an EC2 instance and creating a S3 bucket. However, it is by no means a source of truth guide to working with AWS, it will only provide high level steps. The best place to learn about how to use AWS is through the [Amazon documentation](https://aws.amazon.com/documentation/).

# Hadoop and S3

A majority of Gobblin's code base uses Hadoop's [FileSystem](https://hadoop.apache.org/docs/r2.4.1/api/org/apache/hadoop/fs/FileSystem.html) object to read and write data. The `FileSystem` object is an abstract class, and typical implementations either write to the local file system, or write to HDFS. There has been significant work to create an implementation of the `FileSystem` object that reads and writes to S3. The best guide to read about the different S3 `FileSystem` implementations is [here](https://wiki.apache.org/hadoop/AmazonS3).

There are a few different S3 `FileSystem` implementations, the two of note are the `s3a` and the `s3` file systems. The `s3a` file system is relatively new and is only available in Hadoop 2.6.0 (see the original [JIRA](https://issues.apache.org/jira/browse/HADOOP-10400) for more information). The `s3` filesystem has been around for a while.

## The `s3a` File System

The `s3a` file system uploads files to a specified bucket. The data uploaded to S3 via this file system is interoperable with other S3 tools. However, there are a few caveats when working with this file system:

* Since S3 does not support renaming of files in a bucket, the `S3AFileSystem.rename(Path, Path)` operation will actually copy data from the source `Path` to the destination `Path`, and then delete the source `Path` (see the [source code](http://grepcode.com/file/repo1.maven.org/maven2/org.apache.hadoop/hadoop-aws/2.6.0/org/apache/hadoop/fs/s3a/S3AFileSystem.java) for more information)
* When creating a file using `S3AFileSystem.create(...)` data will be first written to a staging file on the local file system, and when the file is closed, the staging file will be uploaded to S3 (see the [source code](http://grepcode.com/file/repo1.maven.org/maven2/org.apache.hadoop/hadoop-aws/2.6.0/org/apache/hadoop/fs/s3a/S3AOutputStream.java) for more information)

Thus, when using the `s3a` file system with Gobblin it is recommended that one configures Gobblin to first write its staging data to the local filesystem, and then to publish the data to S3. The reason this is the recommended approach is that each Gobblin `Task` will write data to a staging file, and once the file has been completely written it publishes the file to a output directory (it does this by using a rename function). Finally, the `DataPublisher` moves the files from the staging directory to its final directory (again done using a rename function). This requires two renames operations and would be very inefficient if a `Task` wrote directly to S3.

Furthermore, writing directly to S3 requires creating a staging file on the local file system, and then creating a `PutObjectRequest` to upload the data to S3. This is logically equivalent to just configuring Gobblin to write to a local file and then publishing it to S3.

## The `s3` File System

The `s3` file system stores file as blocks, similar to how HDFS stores blocks. This makes renaming of files more efficient, but data written using this file system is not interoperable with other S3 tools. This limitation may make using this file system less desirable, so the majority of this document focuses on the `s3a` file system. Although the majority of the walkthrough should apply for the `s3` file system also.

# Getting Gobblin to Publish to S3

This section will provide a step by step guide to setting up an EC2 instance, a S3 bucket, installing Gobblin on EC2, and configuring Gobblin to publish data to S3.

This guide will use the free-tier provided by AWS to setup EC2 and S3.

## Signing Up For AWS

In order to use EC2 and S3, one first needs to sign up for an AWS account. The easiest way to get started with AWS is to use their [free tier](https://aws.amazon.com/free/).

## Setting Up EC2

### Launching an EC2 Instance

Once you have an AWS account, login to the AWS [console](https://console.aws.amazon.com/console/home). Select the EC2 link, which will bring you to the [EC2 dashboard](https://console.aws.amazon.com/ec2/).

Click on `Launch Instance` to create a new EC2 instance. Before the instance actually starts to run, there area a few more configuration steps necessary:

* Choose an Amazon Machine Image ([AMI](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIs.html))
    * For this walkthrough we will pick Red Hat Enterprise Linux ([RHEL](https://en.wikipedia.org/wiki/Red_Hat_Enterprise_Linux)) AMI
* Choose an Instance Type
    * Since this walkthrough uses the Amazon Free Tier, we will pick the General Purpose `t2.micro` instance
        * This instance provides us with 1 vCPU and 1 GiB of RAM
    * For more information on other instance types, check out the AWS [docs](https://aws.amazon.com/ec2/instance-types/)
* Click Review and Launch
    * We will use the defaults for all other setting options
    * When reviewing your instance, you will most likely get a warning saying access to your EC2 instance is open to the world
    * If you want to fix this you have to edit the [Security Groups](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html); how to do that is out of the scope of this document
* Set Up SSH Keys
    * After reviewing your instance, click `Launch`
    * You should be prompted to setup [SSH](https://en.wikipedia.org/wiki/Secure_Shell) keys
    * Use an existing key pair if you have one, otherwise create a new one and download it
* SSH to Launched Instance
    * SSH using the following command: `ssh -i my-private-key-file.pem ec2-user@instance-name`
        * The `instance-name` can be taken from the `Public DNS` field from the instance information
        * SSH may complain that the private key file has insufficient permissions
            * Execute `chmod 600 my-private-key-file.pem` to fix this
        * Alternatively, one can modify the `~/.ssh/config` file instead of specifying the `-i` option

After following the above steps, you should be able to freely SSH into the launched EC2 instance, and monitor / control the instance from the [EC2 dashboard](https://console.aws.amazon.com/ec2/).

### EC2 Package Installations

Before setting up Gobblin, you need to install [Java](https://en.wikipedia.org/wiki/Java_(programming_language)) first. Depending on the AMI instance you are running Java may or may not already be installed (you can check if Java is already installed by executing `java -version`).

#### Installing Java

* Execute `sudo yum install java-1.8.0-openjdk*` to install Open JDK 8
* Confirm the installation was successful by executing `java -version`
* Set the `JAVA_HOME` environment variable in the `~/.bashrc/` file
    * The value for `JAVA_HOME` can be found by executing `` readlink `which java` ``

## Setting Up S3

Go to the [S3 dashboard](https://console.aws.amazon.com/s3)

* Click on `Create Bucket`
    * Enter a name for the bucket (e.g. `gobblin-demo-bucket`)
    * Enter a [Region](http://docs.aws.amazon.com/general/latest/gr/rande.html) for the bucket (e.g. `US Standard`)

## Setting Up Gobblin on EC2

* Download and Build Gobblin Locally
    * On your local machine, clone the [Gobblin repository](https://github.com/apache/incubator-gobblin): `git clone git@github.com:apache/incubator-gobblin.git` (this assumes you have [Git](https://en.wikipedia.org/wiki/Git_(software)) installed locally)
    * Build Gobblin using the following commands (it is important to use Hadoop version 2.6.0 as it includes the `s3a` file system implementation):
```
cd gobblin
./gradlew clean build -PhadoopVersion=2.6.0 -x test
```
* Upload the Gobblin Tar to EC2
    * Execute the command: 
```
scp -i my-private-key-file.pem gobblin-dist-[project-version].tar.gz ec2-user@instance-name:
```
* Un-tar the Gobblin Distribution
    * SSH to the EC2 Instance
    * Un-tar the Gobblin distribution: `tar -xvf gobblin-dist-[project-version].tar.gz`
* Download AWS Libraries
    * A few JARs need to be downloaded using some cURL commands:
```
curl http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar > gobblin-dist/lib/aws-java-sdk-1.7.4.jar
curl http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.6.0/hadoop-aws-2.6.0.jar > gobblin-dist/lib/hadoop-aws-2.6.0.jar
```

## Configuring Gobblin on EC2

Assuming we are running Gobblin in [standalone mode](../user-guide/Gobblin-Deployment#Standalone-Deployment), the following configuration options need to be modified in the file `gobblin-dist/conf/gobblin-standalone.properties`.

* Add the key `data.publisher.fs.uri` and set it to `s3a://gobblin-demo-bucket/`
    * This configures the job to publish data to the S3 bucket named `gobblin-demo-bucket`
* Add the AWS Access Key Id and Secret Access Key
    * Set the keys `fs.s3a.access.key` and `fs.s3a.secret.key` to the appropriate values
    * These keys correspond to [AWS security credentials](http://docs.aws.amazon.com/general/latest/gr/aws-security-credentials.html)
    * For information on how to get these credentials, check out the AWS documentation [here](http://docs.aws.amazon.com/general/latest/gr/aws-security-credentials.html)
    * The AWS documentation recommends using [IAM roles](http://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html); how to set this up is out of the scope of this document; for this walkthrough we will use root access credentials

## Launching Gobblin on EC2

Assuming we want Gobblin to run in standalone mode, follow the usual steps for [standalone deployment](../user-guide/Gobblin-Deployment#Standalone-Deployment).

For the sake of this walkthrough, we will launch the Gobblin [wikipedia example](https://github.com/apache/incubator-gobblin/blob/master/gobblin-example/src/main/resources/wikipedia.pull). Directions on how to run this example can be found [here](../Getting-Started). The command to launch Gobblin should look similar to:
```
sh bin/gobblin standalone start --conf-dir /home/ec2-user/gobblin-dist/config
```

If you are running on the Amazon free tier, you will probably get an error in the `nohup.out` file saying there is insufficient memory for the JVM. To fix this add `--jvmflags "-Xms256m -Xmx512m"` to the `start` command.

Data should be written to S3 during the publishing phase of Gobblin. One can confirm data was successfully written to S3 by looking at the [S3 dashboard](https://console.aws.amazon.com/s3).

### Writing to S3 Outside EC2

It is possible to write to an S3 bucket outside of an EC2 instance. The setup steps are similar to walkthrough outlined above. For more information on writing to S3 outside of AWS, check out [this article](https://aws.amazon.com/articles/5050).

## Configuration Properties for `s3a`

The `s3a` FileSystem has a number of configuration properties that can be set to tune the behavior and performance of the `s3a` FileSystem. A complete list of the properties can be found here: https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html

# FAQs

### How do I control the directory the `s3a` uses when writing to local disk

The configuration property `fs.s3a.buffer.dir` controls the location where the `s3a` FileSystem will write data locally before uplodaing it to S3. By default, this property is set to `${hadoop.tmp.dir}/s3a`.
