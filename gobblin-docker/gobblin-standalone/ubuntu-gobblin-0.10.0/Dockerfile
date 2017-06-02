FROM gobblin/gobblin-distributions:ubuntu-gobblin-0.10.0

RUN mkdir -p /etc/opt/job-conf
RUN mkdir -p /home/gobblin/work-dir

ENV GOBBLIN_WORK_DIR /home/gobblin/work-dir
ENV GOBBLIN_JOB_CONFIG_DIR /etc/opt/job-conf

CMD ["java", "-cp", "/opt/gobblin/gobblin-dist/lib/*", "-Dlog4j.configuration=file:/opt/gobblin/gobblin-dist/conf/log4j-standalone.xml", "-Dgobblin.logs.dir=/var/log/gobblin", "-Dorg.quartz.properties=/opt/gobblin/gobblin-dist/conf/quartz.properties", "gobblin.scheduler.SchedulerDaemon", "/opt/gobblin/gobblin-dist/conf/gobblin-standalone.properties"]
