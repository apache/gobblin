FROM adoptopenjdk/openjdk8:jdk8u292-b10-debian
LABEL maintainer="i@koosha.io"

COPY docker-entrypoint.sh /
COPY app.jar /

RUN chmod +x /docker-entrypoint.sh

VOLUME ["/jars"]

EXPOSE 8080

ENTRYPOINT ["/docker-entrypoint.sh"]

