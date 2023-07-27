FROM ubuntu:22.04 as builder

ADD ./ /vectordb
RUN apt-get update && \
    apt-get install -y python3 python3-pip git cmake build-essential libboost-all-dev && \
    git clone https://github.com/oatpp/oatpp.git && \
    mkdir oatpp/build && cd oatpp/build && cmake .. && make && make install && \
    cd ../.. && rm -rf oatpp && \
    mkdir build && cd build && cmake .. && make && chmod +x vectordb

FROM ubuntu:22.04
COPY --from=builder /vectordb/engine/build/vectordb /

ENTRYPOINT ["/vectordb"]
    
