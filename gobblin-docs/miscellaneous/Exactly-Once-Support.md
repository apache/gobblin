Table of Contents
-----------------

[TOC]

This page outlines the design for exactly-once support in Gobblin. 

Currently the flow of publishing data in Gobblin is:

1. DataWriter writes to staging folder 
2. DataWriter moves files from staging folder to task output folder
3. Publisher moves files from task output folder to job output folder
4. Persists checkpoints (watermarks) to state store
5. Delete staging folder and task-output folder.

This flow does not theoretically guarantee exactly-once delivery, rather, it guarantess at least once. Because if something bad happens in step 4, or between steps 3 and 4, it is possible that data is published but checkpoints are not, and the next run will re-extract and re-publish those records.

To guarantee exactly-once, steps 3 & 4 should be atomic.

## Achieving Exactly-Once Delivery with `CommitStepStore`

The idea is similar as write-head logging. Before doing the atomic steps (i.e., steps 3 & 4), first write all these steps (referred to as `CommitStep`s) into a `CommitStepStore`. In this way, if failure happens during the atomic steps, the next run can continue doing the rest of the steps before ingesting more data for this dataset.

**Example**: Suppose we have a Kafka-HDFS ingestion job, where each Kafka topic is a dataset. Suppose a task generates three output files for topic 'MyTopic':

```
task-output/MyTopic/2015-12-09/1.avro
task-output/MyTopic/2015-12-09/2.avro
task-output/MyTopic/2015-12-10/1.avro
```

which should be published to
```
job-output/MyTopic/2015-12-09/1.avro
job-output/MyTopic/2015-12-09/2.avro
job-output/MyTopic/2015-12-10/1.avro
```

And suppose this topic has two partitions, and the their checkpoints, i.e., the actual high watermarks are `offset=100` and `offset=200`.

In this case, there will be 5 CommitSteps for this dataset:

1. `FsRenameCommitStep`: rename `task-output/MyTopic/2015-12-09/1.avro` to `job-output/MyTopic/2015-12-09/1.avro`
2. `FsRenameCommitStep`: rename `task-output/MyTopic/2015-12-09/2.avro` to `job-output/MyTopic/2015-12-09/2.avro`
3. `FsRenameCommitStep`: rename `task-output/MyTopic/2015-12-10/1.avro` to `job-output/MyTopic/2015-12-10/1.avro`
4. `HighWatermarkCommitStep`: set the high watermark for partition `MyTopic:0 = 100`
5. `HighWatermarkCommtiStep`: set the high watermark for partition `MyTopic:1 = 200`

If all these `CommitStep`s are successful, we can proceed with deleting task-output folder and deleting the above `CommitStep`s from the `CommitStepStore`. If any of these steps fails, these steps will not be deleted. When the next run starts, for each dataset, it will check whether there are `CommitStep`s for this dataset in the CommitStepStore. If there are, it means the previous run may not have successfully executed some of these steps, so it will verify whether each step has been done, and re-do the step if not. If the re-do fails for a certain number of times, this dataset will be skipped. Thus the `CommitStep` interface will have two methods: `verify()` and `execute()`.

## Scalability

The above approach potentially affects scalability for two reasons:

1. The driver needs to write all `CommitStep`s to the `CommitStepStore` for each dataset, once it determines that all tasks for the dataset have finished. This may cause scalability issues if there are too many `CommitStep`s, too many datasets, or too many tasks.
2. Upon the start of the next run, the driver needs to verify all `CommitStep`s and redo the `CommitStep`s that the previous run failed to do. This may also cause scalability issues if there are too many `CommitStep`s.

Both issues can be resolved by moving the majority of the work to containers, rather than doing it in the driver. 

For #1, we can make each container responsible for writing `CommitStep`s for a subset of the datasets. Each container will keep polling the `TaskStateStore` to determine whether all tasks for each dataset that it is responsible for have finished, and if so, it writes `CommitStep`s for this dataset to the `CommitStepStore`.

 #2 can also easily be parallelized where we have each container responsible for a subset of datasets.

## APIs

**CommitStep**:
``` java
/**
 * A step during committing in a Gobblin job that should be atomically executed with other steps.
 */
public abstract class CommitStep {

  private static final Gson GSON = new Gson();

  public static abstract class Builder<T extends Builder<?>> {
  }

  protected CommitStep(Builder<?> builder) {
  }

  /**
   * Verify whether the CommitStep has been done.
   */
  public abstract boolean verify() throws IOException;

  /**
   * Execute a CommitStep.
   */
  public abstract boolean execute() throws IOException;

  public static CommitStep get(String json, Class<? extends CommitStep> clazz) throws IOException {
    return GSON.fromJson(json, clazz);
  }
}
```

**CommitSequence**:
``` java
@Slf4j
public class CommitSequence {
  private final String storeName;
  private final String datasetUrn;
  private final List<CommitStep> steps;
  private final CommitStepStore commitStepStore;

  public CommitSequence(String storeName, String datasetUrn, List<CommitStep> steps, CommitStepStore commitStepStore) {
    this.storeName = storeName;
    this.datasetUrn = datasetUrn;
    this.steps = steps;
    this.commitStepStore = commitStepStore;
  }

  public boolean commit() {
    try {
      for (CommitStep step : this.steps) {
        if (!step.verify()) {
          step.execute();
        }
      }
      this.commitStepStore.remove(this.storeName, this.datasetUrn);
      return true;
    } catch (Throwable t) {
      log.error("Commit failed for dataset " + this.datasetUrn, t);
      return false;
    }
  }
}
```

**CommitStepStore**:
``` java
/**
 * A store for {@link CommitStep}s.
 */
public interface CommitStepStore {

  /**
   * Create a store with the given name.
   */
  public boolean create(String storeName) throws IOException;

  /**
   * Create a new dataset URN in a store.
   */
  public boolean create(String storeName, String datasetUrn) throws IOException;

  /**
   * Whether a dataset URN exists in a store.
   */
  public boolean exists(String storeName, String datasetUrn) throws IOException;

  /**
   * Remove a given store.
   */
  public boolean remove(String storeName) throws IOException;

  /**
   * Remove all {@link CommitStep}s for the given dataset URN from the store.
   */
  public boolean remove(String storeName, String datasetUrn) throws IOException;

  /**
   * Put a {@link CommitStep} with the given dataset URN into the store.
   */
  public boolean put(String storeName, String datasetUrn, CommitStep step) throws IOException;

  /**
   * Get the {@link CommitSequence} associated with the given dataset URN in the store.
   */
  public CommitSequence getCommitSequence(String storeName, String datasetUrn) throws IOException;

}
```
