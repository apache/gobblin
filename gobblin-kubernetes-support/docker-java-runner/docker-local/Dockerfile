FROM adoptopenjdk/openjdk8:jdk8u292-b10-debian
LABEL maintainer="i@koosha.io"

COPY "start.sh" /
COPY runner.jar /

RUN chmod +x "/start.sh"

VOLUME ["/workspace"]

#VOLUME ["/jars"]
#VOLUME ["/input"]
#VOLUME ["/output"]

ENTRYPOINT ["/start.sh"]

