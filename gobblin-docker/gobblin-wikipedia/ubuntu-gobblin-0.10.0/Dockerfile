FROM gobblin/gobblin-distributions:ubuntu-gobblin-0.10.0

ENV RELEASE_VERSION "0.10.0"
ENV GOBBLIN_WORK_DIR /home/gobblin/work-dir

RUN echo "Downloading Gobblin Wikipedia Configuration Files"
RUN mkdir -p /etc/opt/gobblin/job-conf

# Downloading wikipedia.pull
RUN curl -L --progress-bar https://raw.githubusercontent.com/linkedin/gobblin/gobblin_$RELEASE_VERSION/gobblin-example/src/main/resources/wikipedia.pull -o /etc/opt/gobblin/job-conf/wikipedia.pull

# Start Gobblin Wikipedia Example
CMD ["java", "-cp", "/opt/gobblin/gobblin-dist/lib/*", "-Dlog4j.configuration=file:/opt/gobblin/gobblin-dist/conf/log4j.properties", "gobblin.runtime.local.CliLocalJobLauncher", "--sysconfig", "/opt/gobblin/gobblin-dist/conf/gobblin-cli.properties", "--jobconfig", "/etc/opt/gobblin/job-conf/wikipedia.pull"]
