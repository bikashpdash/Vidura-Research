FROM ubuntu:focal-20220826
RUN apt update -y
RUN apt install -y curl wget
RUN curl -fsSL https://deb.nodesource.com/setup_16.x | bash -
RUN apt install -y nodejs
RUN npm i -g http-server
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata
RUN apt install -y build-essential protobuf-compiler python3 python-is-python3 libprotobuf-dev libcurl4-openssl-dev libboost-all-dev m4 g++ libssl-dev libjemalloc-dev
RUN wget https://download.rethinkdb.com/repository/raw/dist/rethinkdb-2.4.2.tgz
RUN tar xf rethinkdb-2.4.2.tgz
WORKDIR /rethinkdb-2.4.2
RUN ./configure --allow-fetch --fetch boost
RUN make -j8
CMD ["http-server"]