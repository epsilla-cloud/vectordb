FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y python3 python3-pip git cmake build-essential libboost-all-dev && \
    git clone https://github.com/oatpp/oatpp.git && \
    mkdir oatpp/build && cd oatpp/build && cmake .. && make && make install && \
    cd .. && rm -rf oatpp && \
    bash build.sh && \
    chmod +x build/vectordb

ADD ./build/vectordb  /

ENTRYPOINT ["/vectordb", "-c"]
    
