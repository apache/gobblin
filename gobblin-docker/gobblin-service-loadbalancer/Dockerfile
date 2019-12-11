ARG GOBBLIN_ROOT=${GOBBLIN_REPO_ROOT}

FROM openjdk:8-alpine as build
WORKDIR /home/gobblin
ARG GOBBLIN_VERSION=latest
COPY ${GOBBLIN_ROOT} .
RUN apk add --no-cache bash && \
    ./gradlew :gobblin-distribution:buildDistributionTar -Pversion=latest && \
    tar -xvf apache-gobblin-incubating-bin-${GOBBLIN_VERSION}.tar.gz && \
    rm apache-gobblin-incubating-bin-${GOBBLIN_VERSION}.tar.gz

FROM openjdk:8-jre-alpine
WORKDIR /home/gobblin/
COPY --from=build /home/gobblin/gobblin-dist .
RUN apk add --no-cache bash

CMD ["./bin/gobblin-service-load-balancer.sh"]