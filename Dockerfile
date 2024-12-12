#Use Ubuntu 22.04 as the base image
FROM ubuntu:22.04
# Install necessary packages and clean up in a single layer to reduce image size
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    iproute2 \
    binutils \
    tcpdump \
    iputils-ping \
    net-tools \
    python3-minimal \
    curl \
    && wget https://github.com/hairyhenderson/gomplate/releases/download/v3.11.7/gomplate_linux-$(dpkg --print-architecture) -O /usr/local/bin/gomplate \
    && chmod +x /usr/local/bin/gomplate \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# Copy the Envoy binary
COPY envoy-preprod-image /usr/bin/envoy
# Create volume
VOLUME ["/etc/envoy", "/var/log"]