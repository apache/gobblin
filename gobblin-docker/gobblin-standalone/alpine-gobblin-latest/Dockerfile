ARG GOBBLIN_ROOT=${GOBBLIN_REPO_ROOT}

FROM openjdk:8-alpine as build
WORKDIR /home/gobblin
ARG GOBBLIN_VERSION=latest
COPY ${GOBBLIN_ROOT} .
RUN apk add --no-cache bash && \
    wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub && \
    wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.32-r0/glibc-2.32-r0.apk && \
    apk add glibc-2.32-r0.apk && \
    ./gradlew :gobblin-distribution:buildDistributionTar -Pversion=latest && \
    tar -xvf apache-gobblin-incubating-bin-${GOBBLIN_VERSION}.tar.gz && \
    rm apache-gobblin-incubating-bin-${GOBBLIN_VERSION}.tar.gz

FROM openjdk:8-jre-alpine
WORKDIR /home/gobblin/
COPY ${GOBBLIN_ROOT}/gobblin-docker/gobblin-standalone/alpine-gobblin-latest/entrypoint.sh ./bin/entrypoint.sh
COPY --from=build /home/gobblin/gobblin-dist .
RUN apk add --no-cache bash && \
    mkdir /tmp/gobblin-standalone && \
    mkdir /tmp/gobblin-standalone/jobs && \
    chmod +x ./bin/entrypoint.sh
ENV GOBBLIN_WORK_DIR=/tmp/gobblin-standalone/jobs
ENV GOBBLIN_JOB_CONFIG_DIR=/tmp/gobblin-standalone/jobs

CMD ["./bin/entrypoint.sh"]