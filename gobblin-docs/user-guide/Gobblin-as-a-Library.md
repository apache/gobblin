Table of Contents
--------------------

[TOC]

Using Gobblin as a Library
-----------------------

A Gobblin ingestion flow can be embedded into a java application using the `EmbeddedGobblin` class.

The following code will run a Hello-World Gobblin job as an embedded application using a template. This will simply print "Hello World \<i\>!" to stdout a few times.
```java
EmbeddedGobblin embeddedGobblin = new EmbeddedGobblin("TestJob")
        .setTemplate(ResourceBasedJobTemplate.forResourcePath("templates/hello-world.template"));
JobExecutionResult result = embeddedGobblin.run();
```

Note: `EmbeddedGobblin` starts and destroys an embedded Gobblin instance every time `run()` is called. If an application needs to run a large number of Gobblin jobs, it should instantiate and manage its own Gobblin driver.

Creating an Embedded Gobblin instance
-----------------------------------

The code snippet above creates an `EmbeddedGobblin` instance. This instance can run arbitrary Gobblin ingestion jobs, and allows the use of templates. However, the user needs to configure the job by using the exact key needed for each feature.

An alternative is to use a subclass of `EmbeddedGobblin` which provides methods to more easily configure the job. For example, an easier way to run a Gobblin distcp job is to use `EmbeddedGobblinDistcp`:
```java
EmbeddedGobblinDistcp distcp = new EmbeddedGobblinDistcp(sourcePath, targetPath).delete();
distcp.run();
```
This subclass automatically knows which template to use, the required configurations for the job (which are included as constructor parameters), and also provides convenience methods for the most common configurations (in the case above, the method `delete()` instructs the job to delete files that exist in the target but not the source).

The following is a non-extensive list of available subclasses of `EmbeddedGobblin`:
* `EmbeddedGobblinDistcp`: distributed copy between Hadoop compatible file systems.
* `EmbeddedWikipediaExample`: a getting started example that pulls page updated from Wikipedia.

Configuring Embedded Gobblin
---------------------------

`EmbeddedGobblin` allows any configuration that a standalone Gobblin job would allow. `EmbeddedGobblin` itself provides a few convenience methods to alter the behavior of the Gobblin framework. Other methods allow users to set a job template to use or set job level configurations.

|Method|Parameters|Description|
|-------------|-------------|-------------|
|`mrMode`| N/A | Gobblin should run on MR mode. |
|`setTemplate`| Template object to use | Use a job template.|
|`useStateStore` | State store directory | By default, embedded Gobblin is stateless and disables state store. This method enables the state store at the indicated location allowing using watermarks from previous jobs. |
|`distributeJar` | Path to jar in local fs | Indicates that a specific jar is needed by Gobblin workers when running in distributed mode (e.g. MR mode). Gobblin will automatically add this jar to the classpath of the workers. |
|`setConfiguration` | key - value pair | Sets a job level configuration. |
|`setJobTimeout` | timeout and time unit, or ISO period | Sets the timeout for the Gobblin job. `run()` will throw a `TimeoutException` if the job is not done after this period. (Default: 10 days) |
|`setLaunchTimeout` | timeout and time unit, or ISO period | Sets the timeout for launching Gobblin job. `run()` will throw a `TimeoutException` if the job has not started after this period. (Default: 10 seconds) |
|`setShutdownTimeout` | timeout and time unit, or ISO period | Sets the timeout for shutting down embedded Gobblin after the job has finished. `run()` will throw a `TimeoutException` if the method has not returned within the timeout after the job finishes. Note that a `TimeoutException` may indicate that Gobblin could not release JVM resources, including threads. |

Additional to the above, subclasses of `EmbeddedGobblin` might offer their own convenience methods.

Running Embedded Gobblin
-----------------------

After `EmbeddedGobblin` has been configured it can be run with one of two methods:
* `run()`: blocking call. Returns a `JobExecutionResult` after the job finishes and Gobblin shuts down.
* `runAsync()`: asynchronous call. Returns a `JobExecutionDriver`, which implements `Future<JobExecutionResult>`.

Extending Embedded Gobblin
-------------------------
Developers can extend `EmbeddedGobblin` to provide users with easier ways to launch a particular type of job. For an example see `EmbeddedGobblinDistcp`.

Best practices:
* Generally, a subclass of `EmbeddedGobblin` is based on a template. The template should be automatically loaded on construction and the constructor should call `setTemplate(myTemplate)`.
* All required configurations for a job should be parsed from the constructor arguments. User should be able to run `new MyEmbeddedGobblinExtension(params...).run()` and get a sensible job run.
* Convenience methods should be added for the most common configurations users would want to change. In general a convenience method will call a few other methods transparently to the user. For example:
```java
  public EmbeddedGobblinDistcp simulate() {
    this.setConfiguration(CopySource.SIMULATE, Boolean.toString(true));
    return this;
  }
```
* If the job requires additional jars in the workers that are not part of the minimal Gobblin ingestion classpath (see `EmbeddedGobblin#getCoreGobblinJars` for this list), then the constructor should call `distributeJar(myJar)` for the additional jars.
