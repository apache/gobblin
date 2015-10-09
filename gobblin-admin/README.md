# Gobblin Interfaces
Gobblin comes with two tools for better understanding the state of
Gobblin and the jobs executed. These interfaces are early in their
development, will likely change, and have many new features planned. The
two interfaces provided are a command line interface and a GUI,
accessible via a web server. The current state of the interfaces relies
on the [Job Execution History
Store](https://github.com/linkedin/gobblin/wiki/Job%20Execution%20History%20Store),
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
History Store. The simplest form of deployment in its current state is
to use a lightweight HTTP server such as Python's SimpleHTTPServer,
since the frontend is simply static files. However, to closer integrate
to Gobblin, it could be deployed with Jetty on Gobblin start, or with
any other web server. The root directory to be served is at
`gobblin-interface/src/main/web`. The URL scheme is RESTful, so job
information can be found at `<host:port>/job/JobName` and details about
a job id can be found at `<host:port>/job-details/JobId`.

The GUI is in the earliest stage of development and is not very
extensible. If you are serving your history store API in a non-default
location, you will have to create the settings file, found at
`gobblin-interface/src/main/web/js/settings.js`. The contents of this
file should be mirrored after
[`gobblin-interface/src/main/web/js/settings.js.example`](https://github.com/linkedin/gobblin/blob/feature/gui/gobblin-interface/src/main/web/js/settings.js.example).
As the project progresses, this file will be used for more custom
configuration, and its creation/population should likely be automated by
a deploy tool.

Note that `settings.js` is not required by the frontend (by default it
looks for the Gobblin default location for the API at `localhost:8080`). 


## Readme Todo:
- Update branch when pushed to master, update links when pushed to
  linkedin
- Merge `settings.js.example` and update link
