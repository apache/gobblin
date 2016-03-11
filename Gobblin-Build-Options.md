# Table of Contents

- [Introduction](#introduction)
- [Options](#options)
    - [Versions](#versions)
      - [Hadoop Version](#hadoop-version)
      - [Hive Version](#hive-version)
      - [Pegasus Version](#pegasus-version)
      - [Byteman Version](#byteman-version)
    - [Exclude Hadoop Dependencies from `gobblin-dist.tar.gz`](#exclude-hadoop-dependencies-from-gobblin-disttargz)
    - [Exclude Hive Dependencies from `gobblin-dist.tar.gz`](#exclude-hive-dependencies-from-gobblin-disttargz)
- [Custom Gradle Tasks](#custom-gradle-tasks)
    - [Print Project Dependencies](#print-project-dependencies)
- [Useful Gradle Commands](#useful-gradle-commands)
    - [Skipping Tests](#skipping-tests)

# Introduction

This page outlines all the options that can be specified when building Gobblin using Gradle. The typical way of building Gobblin is to run:
```
./gradlew build
```
However, there are a number of parameters that can be passed into the above command to customize the build process.

# Options

These options just need to be added to the command above to take effect.

### Versions

#### Hadoop Version

The Hadoop version can be specified by adding the option `-PhadoopVersion=[my-hadoop-version]`. If using a Hadoop version over `2.0.0` the option `-PuseHadoop2` must also be added.

#### Hive Version

The Hive version can be specified by adding the option `-PhiveVersion=[my-hive-version]`.

#### Pegasus Version

The Pegasus version can be specified by adding the option `-PpegasusVersion=[my-pegasus-version]`.

#### Byteman Version

The Byteman version can be specified by adding the option `-PbytemanVersion=[my-byteman-version]`.

### Exclude Hadoop Dependencies from `gobblin-dist.tar.gz`

Add the option `-PexcludeHadoopDeps` to exclude all Hadoop libraries from `gobblin-dist.tar.gz`.

### Exclude Hive Dependencies from `gobblin-dist.tar.gz`

Add the option `-PexcludeHiveDeps` to exclude all Hadoop libraries from `gobblin-dist.tar.gz`.

# Custom Gradle Tasks

A few custom built Gradle tasks.

### Print Project Dependencies

Executing this command will print out all the dependencies between the different Gobblin Gradle sub-projects: `./gradlew dotProjectDependencies`.

# Useful Gradle Commands

These commands make working with Gradle a little easier.

### Skipping Tests

Add `-x test` to the end of the build command.