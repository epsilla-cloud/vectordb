FROM epsilla/base as builder
ADD ./ /vectordb
WORKDIR /vectordb
ENV OATPP_INSTALL_PATH=/vectordb/build/dependencies
RUN scripts/install_oatpp_modules.sh
RUN mkdir -p /vectordb/build && cd /vectordb/build && cmake .. && make && chmod +x vectordb


FROM epsilla/base
ARG TARGETARCH
ARG RELEASE_VERSION=latest
ENV ENV_RELEASE_VERSION=$RELEASE_VERSION
COPY --from=builder /vectordb/build/vectordb /vectordb
COPY ./scripts/heartbeat.sh /heartbeat.sh
HEALTHCHECK --interval=600s --timeout=30s --retries=1000 CMD bash /heartbeat.sh || exit 0
ENTRYPOINT ["/vectordb"]
