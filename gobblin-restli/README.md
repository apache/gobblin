Gobblin Rest.li
===============

This directory is intended for subprojects using [Rest.li](https://github.com/linkedin/rest.li).

To create a rest.li subproject, create a directory under `gobblin-restli` with the name of your service, and create `api`, `server`, and `client` subdirectories under it. 
The directory structure would be:
 
```
gobblin-restli/
\-- my-restli-service/
   |-- api/
   |-- server/
   \-- client/  
```

The correct rest.li gradle properties will be automatically applied to generate the java classes, establish the correct dependencies between the submodules, and publish the artifacts.
No `build.gradle` files are needed unless you need to override the default options.
