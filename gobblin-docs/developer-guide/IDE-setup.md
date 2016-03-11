Table of Contents
-----------------------------------------------

[TOC]

# Introduction
This document is for users who want to import the Gobblin code base into an [IDE](https://en.wikipedia.org/wiki/Integrated_development_environment) and directly modify that Gobblin code base. This is not for users who want to just setup Gobblin as a Maven dependency.

# IntelliJ Integration
Gobblin uses standard build tools to import code into an IntelliJ project. Execute the following command to build the necessary `*.iml` files:
```
./gradlew clean idea
```
Once the command finishes, use standard practices to import the project into IntelliJ.

# Eclipse Integration
Gobblin uses standard build tools to import code into an Eclipse project. Execute the following command to build the necessary `*.classpath` and `*.project` files:
```
./gradlew clean eclipse
```
Once the command finishes, use standard practices to import the project into Eclipse.

# Lombok
Gobblin uses [Lombok](https://projectlombok.org/) for reducing boilerplate code. Lombok auto generates boilerplate code at runtime if you are building gobblin from command line.If you are using an IDE, you will see compile errors in some of the classes that use Lombok. Please follow the [IDE setup instructions](https://projectlombok.org/download.html) for your IDE to setup lombok.
