FROM gobblin/gobblin-base:ubuntu

ENV RELEASE_VERSION "0.9.0"

RUN mkdir -p /opt/gobblin/
WORKDIR /opt/gobblin/

# Download gobblin-distribution-$RELEASE_VERSION
RUN curl -OL --progress-bar https://github.com/linkedin/gobblin/releases/download/gobblin_$RELEASE_VERSION/gobblin-distribution-$RELEASE_VERSION.tar.gz

# Un-tar gobblin-distribution-$RELEASE_VERSION
RUN tar -xf gobblin-distribution-$RELEASE_VERSION.tar.gz
