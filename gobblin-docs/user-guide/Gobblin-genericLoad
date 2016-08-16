Table of Contents
-------------------

[TOC]

Overview
--------------------
Previously, the job configuration files could only be loaded from and monitored in the local file system. Efforts have been made to change the limitation and now Gobblin can also load job configuration files in other file systems. Users can easily submit `.pull` files through their preferred file system and specify it in system configuration accordingly. 

This page will use the wikipedia example of Gobblin-standalone interacting with job configuration files in HDFS.


How to submit `.pull` file through HDFS
--------------------
Here are the steps to change the system configuration: 
- Set `fs.uri` to the HDFS uri that the `.pull` file will be submitted to.  
- Use `jobconf.fullyQualifiedPath` to specify the fully qualified location where pull files should be searched for (this replaces the previously used key `jobconf.dir`)

With all these changes to `gobblin-standalone.properties`, you can now submit the `.pull` to the target file system path.  