FROM epsilla/base as builder
ADD ./ /vectordb
WORKDIR /vectordb
ENV OATPP_INSTALL_PATH=/vectordb/build/dependencies
RUN scripts/install_oatpp_modules.sh
RUN mkdir -p /vectordb/build && cd /vectordb/build && cmake .. && make && chmod +x vectordb && \
    wget https://github.com/yandex-cloud/geesefs/releases/latest/download/geesefs-linux-amd64 && \
    chmod +x geesefs-linux-amd64 && mv geesefs-linux-amd64 /usr/local/bin/geesefs


FROM epsilla/base
ARG TARGETARCH
ARG RELEASE_VERSION=latest
ENV ENV_RELEASE_VERSION=$RELEASE_VERSION
COPY --from=builder /vectordb/build/vectordb /vectordb
COPY --from=builder /usr/local/bin/geesefs   /usr/local/bin/geesefs
COPY ./scripts/heartbeat.sh /heartbeat.sh
HEALTHCHECK --interval=600s --timeout=30s --retries=1000 CMD bash /heartbeat.sh || exit 0
ENTRYPOINT ["/vectordb"]
