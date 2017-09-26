# Table of Contents

[TOC]

# Introduction

This page outlines all the options that can be specified when building Gobblin using Gradle. The typical way of building Gobblin is to first checkout the code-base from GitHub and then build the code-base using Gradle.

```
git clone https://github.com/linkedin/gobblin.git
cd gobblin
./gradlew assemble
```

If one wants to compile the code as well as run the tests, use `./gradle assemble test`
or `./gradlew build`.

There are a number of parameters that can be passed into the above command to customize the build process.

# Options

These options just need to be added to the command above to take effect.

### Versions

#### Hadoop Version

The Hadoop version can be specified by adding the option `-PhadoopVersion=[my-hadoop-version]`.

#### Hive Version

The Hive version can be specified by adding the option `-PhiveVersion=[my-hive-version]`.

#### Pegasus Version

The Pegasus version can be specified by adding the option `-PpegasusVersion=[my-pegasus-version]`.

#### Byteman Version

The Byteman version can be specified by adding the option `-PbytemanVersion=[my-byteman-version]`.

### Exclude Hadoop Dependencies from `gobblin-dist.tar.gz`

Add the option `-PexcludeHadoopDeps` to exclude all Hadoop libraries from `gobblin-dist.tar.gz`.

### Exclude Hive Dependencies from `gobblin-dist.tar.gz`

Add the option `-PexcludeHiveDeps` to exclude all Hive libraries from `gobblin-dist.tar.gz`.

# Custom Gradle Tasks

A few custom built Gradle tasks.

### Print Project Dependencies

Executing this command will print out all the dependencies between the different Gobblin Gradle sub-projects: `./gradlew dotProjectDependencies`.

# Useful Gradle Commands

These commands make working with Gradle a little easier.
