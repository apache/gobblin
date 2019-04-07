# Gobblin Interfaces
Gobblin comes with two tools for better understanding the state of
Gobblin and the jobs executed. These interfaces are early in their
development, will likely change, and have many new features planned. The
two interfaces provided are a command line interface and a GUI,
accessible via a web server. The current state of the interfaces relies
on the [Job Execution History
Store](https://gobblin.readthedocs.io/en/latest/user-guide/Job-Execution-History-Store/),
which must be enabled and running for the interfaces to work.

## CLI
Gobblin offers a command line interface out of the box. To run it, build
and run Gobblin, then run `<code_dir>/bin/gobblin-admin.sh`. You'll
likely want to alias that command, along with any additional Java
options that you need to pass in.
### Commands
#### Jobs
Use `gobblin-admin.sh jobs --help` for more additional help and for a
list of all options provided. 

Some common commands include:

|Command
|Result    |
|-----------------------------------------------------------------|----------|
|`gobblin-admin.sh jobs --list`                                   |Lists
distinct job names|
|`gobblin-admin.sh jobs --list --name JobName`                    |Lists
the most recent executions of the given job name|
|`gobblin-admin.sh jobs --details --id job_id`                    |Lists
detailed information about the job execution with the given id|
|`gobblin-admin.sh jobs --properties --<id|name> <job_id|JobName>`|Lists
properties of the job|

#### Tasks
The CLI does not yet support the `tasks` command.

## GUI
The GUI is a lightweight Backbone.js frontend to the Job Execution
History Store. 

To enable it, you can set `admin.server.enabled` to true in your configuration file.
The admin web server will be available on port 8000 by default, but this can be changed with
the `admin.server.port` configuration key.

