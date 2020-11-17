ARG DOCKER_REGISTRY=frdeboffles
ARG DOCKER_IMAGE_VERSION=latest
FROM ${DOCKER_REGISTRY}/node-mongocxx-mapnik:${DOCKER_IMAGE_VERSION} AS builder

WORKDIR /tmp/mapnik-mongo

COPY plugin/* ./

RUN make && make install

###################

FROM node:15-alpine

RUN echo 'http://dl-cdn.alpinelinux.org/alpine/v3.9/main' >> /etc/apk/repositories && \
    apk update && \
    apk add --no-cache \
            freetype \
            icu-libs \
            libjpeg \
            libpng \
            libwebp \
            tiff \
            sqlite \
            sqlite-libs \
            zlib \
            libxml2 \
            harfbuzz \
            cairo \
            libpq \
            postgresql-client && \
    apk add --no-cache \
            boost==1.67.0-r2 && \
    apk add --no-cache \
            boost-program_options==1.67.0-r2



COPY --from=builder /usr/local/include/libmongoc-1.0 /usr/local/include/libmongoc-1.0
COPY --from=builder /usr/local/include/libbson-1.0 /usr/local/include/libbson-1.0
COPY --from=builder /usr/local/include/bsoncxx /usr/local/include/bsoncxx
COPY --from=builder /usr/local/include/mongocxx /usr/local/include/mongocxx
COPY --from=builder /usr/local/lib/lib* /usr/local/lib/
COPY --from=builder /opt/mapnik /opt/mapnik
COPY --from=builder /opt/node-mapnik /opt/node-mapnik
COPY --from=builder /usr/lib/libgdal* /usr/local/lib/
COPY --from=builder /usr/lib/libgeos* /usr/local/lib/
COPY --from=builder /usr/lib/libgif* /usr/local/lib/
COPY --from=builder /usr/bin/gdal* /usr/local/bin/
COPY --from=builder /usr/share/gdal /usr/share/gdal
COPY --from=builder /usr/lib/libproj* /usr/local/lib/
COPY --from=builder /usr/share/proj /usr/share/proj
