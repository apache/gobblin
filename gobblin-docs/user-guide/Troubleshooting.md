Table of Contents
-----------------

[TOC]

## Checking Job State
When there's an issue with a Gobblin job to troubleshoot, it is often helpful to check the state of the job persisted in the state store. Gobblin provides a tool `gobblin-dist/bin/statestore-checker.sh' for checking job states. The tool print job state(s) as a Json document that are easily readable. The usage of the tool is as follows:

```
usage: statestore-checker.sh
 -a,--all                                  Whether to convert all past job
                                           states of the given job
 -i,--id <gobblin job id>                  Gobblin job id
 -kc,--keepConfig                          Whether to keep all
                                           configuration properties
 -n,--name <gobblin job name>              Gobblin job name
 -u,--storeurl <gobblin state store URL>   Gobblin state store root path
                                           URL
``` 

For example, assume that the state store is located at `file://gobblin/state-store/`, to check the job state of the most recent run of a job named "Foo", run the following command:

```
statestore-checker.sh -u file://gobblin/state-store/ -n Foo
``` 

To check the job state of a particular run (say, with job ID job_Foo_123456) of job "Foo", run the following command:

```
statestore-checker.sh -u file://gobblin/state-store/ -n Foo -i job_Foo_123456
```

To check the job states of all past runs of job "Foo", run the following command:

```
statestore-checker.sh -u file://gobblin/state-store/ -n Foo -a
```

To include job configuration in the output Json document, add option `-kc` or `--keepConfig` in the command.

A sample output Json document is as follows:

```
{
	"job name": "GobblinMRTest",
	"job id": "job_GobblinMRTest_1425622600239",
	"job state": "COMMITTED",
	"start time": 1425622600240,
	"end time": 1425622601326,
	"duration": 1086,
	"tasks": 4,
	"completed tasks": 4,
	"task states": [
		{
			"task id": "task_GobblinMRTest_1425622600239_3",
			"task state": "COMMITTED",
			"start time": 1425622600383,
			"end time": 1425622600395,
			"duration": 12,
			"high watermark": -1,
			"retry count": 0
		},
		{
			"task id": "task_GobblinMRTest_1425622600239_2",
			"task state": "COMMITTED",
			"start time": 1425622600354,
			"end time": 1425622600374,
			"duration": 20,
			"high watermark": -1,
			"retry count": 0
		},
		{
			"task id": "task_GobblinMRTest_1425622600239_1",
			"task state": "COMMITTED",
			"start time": 1425622600325,
			"end time": 1425622600344,
			"duration": 19,
			"high watermark": -1,
			"retry count": 0
		},
		{
			"task id": "task_GobblinMRTest_1425622600239_0",
			"task state": "COMMITTED",
			"start time": 1425622600405,
			"end time": 1425622600421,
			"duration": 16,
			"high watermark": -1,
			"retry count": 0
		}
	]
}
```
