FROM node:12-alpine

ARG VERSION=1.15.0
LABEL alpine-version=3.9
LABEL libmongoc-version=${VERSION}

ENV MONGOC_HOME /opt/mongoc
ENV LIBS_HOME /usr/local

RUN apk add --no-cache build-base cmake openssl-dev

WORKDIR ${MONGOC_HOME}
ADD https://github.com/mongodb/mongo-c-driver/releases/download/${VERSION}/mongo-c-driver-${VERSION}.tar.gz mongoc.tar.gz
RUN tar xzf mongoc.tar.gz

WORKDIR ${MONGOC_HOME}/mongo-c-driver-$VERSION/build

RUN cmake -DCMAKE_CXX_STANDARD=17 -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${LIBS_HOME} -DCMAKE_INSTALL_LIBDIR=lib \
  -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF -DENABLE_STATIC=OFF -DENABLE_TESTS=OFF -DENABLE_EXAMPLES=OFF .. \
  && make \
  && make install
