FROM ubuntu:22.04 as builder

RUN apt-get update && \
    apt-get install -y python3 python3-pip git cmake build-essential libboost-all-dev sudo
ADD ./ /vectordb

WORKDIR /vectordb
ENV OATPP_INSTALL_PATH=/vectordb/build/dependencies
RUN scripts/install_oatpp_modules.sh
RUN scripts/setup-dev.sh
RUN mkdir -p /vectordb/build && cd /vectordb/build && cmake .. && make -j $(nproc) && chmod +x vectordb

FROM ubuntu:22.04
RUN apt-get update && apt-get install -y libgomp1 libboost-all-dev
COPY --from=builder /vectordb/build/vectordb /vectordb

ENTRYPOINT ["/vectordb"]
