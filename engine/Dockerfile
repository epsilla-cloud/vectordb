FROM epsilla/base AS builder
ADD ./ /vectordb
WORKDIR /vectordb
ENV OATPP_INSTALL_PATH=/vectordb/build/dependencies
RUN scripts/install_oatpp_modules.sh
RUN mkdir -p /vectordb/build && cd /vectordb/build && cmake .. && make && chmod +x vectordb && curl -o /usr/local/bin/geesefs -L https://github.com/yandex-cloud/geesefs/releases/latest/download/geesefs-linux-amd64 && \
    chmod +x /usr/local/bin/geesefs && /usr/local/bin/geesefs -v
RUN curl -o quickwit.tar.gz -L https://github.com/quickwit-oss/quickwit/releases/download/v0.8.2/quickwit-v0.8.2-x86_64-unknown-linux-gnu.tar.gz && \
    tar -zxvf quickwit.tar.gz && \
    mv quickwit-*/quickwit /usr/local/bin/ && /usr/local/bin/quickwit --version && \
    echo "version: 0.7" > /usr/local/bin/quickwit.config.yaml

FROM epsilla/base
ARG TARGETARCH
ARG RELEASE_VERSION=latest
ENV ENV_RELEASE_VERSION=$RELEASE_VERSION
COPY --from=builder /vectordb/build/vectordb   /vectordb
COPY --from=builder /usr/local/bin/geesefs     /usr/local/bin/geesefs
#COPY --from=builder /usr/local/bin/quickwit   /usr/local/bin/quickwit
#COPY --from=builder /usr/local/bin/quickwit.config.yaml /usr/local/bin/config/quickwit.yaml
COPY ./scripts/heartbeat.sh /heartbeat.sh
HEALTHCHECK --interval=600s --timeout=30s --retries=1000 CMD bash /heartbeat.sh || exit 0
ENTRYPOINT ["/vectordb"]
