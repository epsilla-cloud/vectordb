FROM ubuntu:22.04 as builder

RUN apt-get update && \
    apt-get install -y python3 python3-pip git cmake build-essential libboost-all-dev sudo
ADD ./ /vectordb

WORKDIR /vectordb
ENV OATPP_INSTALL_PATH=/vectordb/build/dependencies
RUN scripts/install_oatpp_modules.sh
RUN mkdir -p /vectordb/build && cd /vectordb/build && cmake .. && make -j $(nproc) && chmod +x vectordb


FROM ubuntu:22.04
ARG TARGETARCH
ARG RELEASE_VERSION=latest
ENV DB_RELEASE_VERSION=$RELEASE_VERSION

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl wget net-tools iproute2 supervisor libgomp1 libboost-all-dev s3fs fuse ca-certificates && \
    echo "====== Install geesefs ======" && \
    wget --no-check-certificate https://github.com/yandex-cloud/geesefs/releases/latest/download/geesefs-linux-${TARGETARCH} && \
    chmod +x geesefs-linux-${TARGETARCH} && \
    mv geesefs-linux-${TARGETARCH} /usr/local/bin/geesefs && \
    geesefs --version && \
    apt-get clean all && rm -rf /var/apt/lists/* /var/cache/apt/*

COPY --from=builder /vectordb/build/vectordb /vectordb
COPY ./scripts/heartbeat.sh /heartbeat.sh
HEALTHCHECK --interval=600s --timeout=30s --retries=1000 CMD bash /heartbeat.sh || exit 0

ENTRYPOINT ["/vectordb"]
