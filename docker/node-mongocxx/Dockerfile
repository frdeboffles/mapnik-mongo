ARG DOCKER_REGISTRY=frdeboffles
ARG DOCKER_IMAGE_VERSION=latest
FROM ${DOCKER_REGISTRY}/node-mongoc:${DOCKER_IMAGE_VERSION}

ARG MONGOCXX_VERSION=r3.6.1
LABEL alpine-version=3.11
LABEL libmongocxx-version=${MONGOCXX_VERSION}
ENV MONGOCXX_HOME /opt/mongocxx

WORKDIR ${MONGOCXX_HOME}
COPY test.cpp .
RUN apk add --no-cache git python3 perl
RUN git clone https://github.com/mongodb/mongo-cxx-driver.git && \
    cd mongo-cxx-driver && \
    git checkout ${MONGOCXX_VERSION}
WORKDIR ${MONGOCXX_HOME}/mongo-cxx-driver/build
RUN cmake -DCMAKE_CXX_STANDARD=17 -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=${LIBS_HOME} -DCMAKE_INSTALL_LIBDIR=lib \
  -DBSONCXX_POLY_USE_MNMLSTC=1 .. \
  && make \
  && make install
WORKDIR ${MONGOCXX_HOME}
RUN export PKG_CONFIG_PATH=${LIBS_HOME}/lib/pkgconfig && \
    c++ --std=c++17 test.cpp -o test $(pkg-config --cflags --libs libmongocxx)
