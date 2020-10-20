ARG GOBBLIN_ROOT=${GOBBLIN_REPO_ROOT}

FROM openjdk:8-alpine as build
WORKDIR /home/gobblin
ARG GOBBLIN_VERSION=latest
COPY ${GOBBLIN_ROOT} .
RUN apk add --no-cache bash && \
    # include libgc in the image so that protobuf generation will work properly
    wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub && \
    wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.32-r0/glibc-2.32-r0.apk && \
    apk add glibc-2.32-r0.apk && \
    ./gradlew :gobblin-distribution:buildDistributionTar -Pversion=latest && \
    tar -xvf apache-gobblin-incubating-bin-${GOBBLIN_VERSION}.tar.gz && \
    rm apache-gobblin-incubating-bin-${GOBBLIN_VERSION}.tar.gz

FROM openjdk:8-jre-alpine
WORKDIR /home/gobblin/
COPY ${GOBBLIN_ROOT}/gobblin-docker/gobblin-service/alpine-gaas-latest/entrypoint.sh ./bin/entrypoint.sh
COPY --from=build /home/gobblin/gobblin-dist .

RUN apk add --no-cache bash && \
    mkdir /tmp/templateCatalog && \
    mkdir /tmp/gobblin-as-service && \
    mkdir /tmp/gobblin-as-service/logs && \
    chmod +x ./bin/entrypoint.sh

CMD ["./bin/entrypoint.sh"]