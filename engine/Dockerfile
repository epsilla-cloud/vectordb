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
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl wget net-tools iproute2 supervisor libgomp1 libboost-all-dev s3fs fuse ca-certificates gcc flex && \
    echo "====== Fix CVE-2023-41915 with  PMIx v4.1.3 ======" && \
    wget --no-check-certificate https://github.com/openpmix/openpmix/releases/download/v4.1.3/pmix-4.1.3.tar.gz && \
    tar -zxvf pmix-4.1.3.tar.gz && cd pmix-4.1.3 && ./configure  --disable-dependency-tracking && make && make install && cd .. && rm -rf pmix-4.1.3* && \
    echo "====== Install geesefs ======" && \
    wget --no-check-certificate https://github.com/yandex-cloud/geesefs/releases/latest/download/geesefs-linux-${TARGETARCH} && \
    chmod +x geesefs-linux-${TARGETARCH} && \
    mv geesefs-linux-${TARGETARCH} /usr/local/bin/geesefs && \
    geesefs --version
COPY --from=builder /vectordb/build/vectordb /vectordb
COPY ./scripts/heartbeat.sh /heartbeat.sh
HEALTHCHECK --interval=600s --timeout=30s --retries=1000 CMD bash /heartbeat.sh || exit 0

ENTRYPOINT ["/vectordb"]
