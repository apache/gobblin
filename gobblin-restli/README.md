Gobblin Rest.li
===============

This directory is intended for subprojects using [Rest.li](https://github.com/linkedin/rest.li).

To create a rest.li subproject, create a directory under `gobblin-restli` with the name of your service, and create `api`, `server`, and `client` subdirectories under it.
For each of those directories, create a soft-link to the corresponding gradle file in `gobblin-restli'.
The directory structure would be:

```
gobblin-restli/
\-- my-restli-service/
   |-- api/
   |   \-- build.gradle -> ../../api.gradle
   |-- server/
   |   \-- build.gradle -> ../../server.gradle
   \-- client/
       \-- build.gradle -> ../../client.gradle
```

The correct rest.li gradle properties will be automatically applied to generate the java classes, establish the correct dependencies between the submodules, and publish the artifacts.
