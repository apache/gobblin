ARG GOBBLIN_ROOT=${GOBBLIN_WORK_DIR}

FROM openjdk:8-alpine as build
WORKDIR /home/gobblin
COPY ${GOBBLIN_ROOT} .
RUN apk add --no-cache bash && \
    ./gradlew :gobblin-distribution:buildDistributionTar -Pversion=latest

FROM openjdk:8-jre-alpine
ARG GOBBLIN_VERSION=latest
WORKDIR /home/gobblin
COPY ${GOBBLIN_ROOT}/gobblin-docker/gobblin-service/alpine-gaas-latest/entrypoint.sh ./gobblin-dist/bin/entrypoint.sh
COPY --from=build /home/gobblin/apache-gobblin-incubating-bin-${GOBBLIN_VERSION}.tar.gz .
RUN tar -xvf apache-gobblin-incubating-bin-${GOBBLIN_VERSION}.tar.gz && \
    mkdir /tmp/templateCatalog/ && \
    chmod +x ./gobblin-dist/bin/gobblin-service.sh && \
    rm apache-gobblin-incubating-bin-${GOBBLIN_VERSION}.tar.gz
ENTRYPOINT ["./gobblin-dist/bin/entrypoint.sh", "start"]