FROM ubuntu:22.04 as builder

RUN apt-get update && \
    apt-get install -y python3 python3-pip git cmake build-essential libboost-dev libboost-all-dev && \
    git clone https://github.com/oatpp/oatpp.git && \
    cd oatpp && git checkout tags/1.3.0 && \
    mkdir build && cd build && \
    cmake -DOATPP_BUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release .. && make && make install && \
    cd ../.. && rm -rf oatpp && \
    apt-get autoremove && apt-get clean
ADD ./ /vectordb
RUN mkdir /vectordb/build && cd /vectordb/build && cmake .. && make && chmod +x vectordb

FROM ubuntu:22.04
RUN apt-get update && apt-get install -y libgomp1
COPY --from=builder /vectordb/build/vectordb /vectordb

ENTRYPOINT ["/vectordb"]
