FROM gobblin/gobblin-base:ubuntu

ENV RELEASE_VERSION "0.7.0"

RUN mkdir -p /opt/gobblin/
WORKDIR /opt/gobblin/

# Download gobblin-distribution
RUN curl -OL --progress-bar https://github.com/linkedin/gobblin/releases/download/gobblin_$RELEASE_VERSION/gobblin-distribution-$RELEASE_VERSION.tar.gz

# Un-tar gobblin-distribution
RUN tar -xf gobblin-distribution-$RELEASE_VERSION.tar.gz
