ARG DOCKER_REGISTRY=frdeboffles
ARG DOCKER_IMAGE_VERSION=latest
FROM ${DOCKER_REGISTRY}/node-mongocxx:${DOCKER_IMAGE_VERSION}

# Update environment PATHs to include mapnik.
ENV PATH="/opt/mapnik/bin:${PATH}"

# Install build & run environment, build mapnik and node-mapnik
# to /opt and then clean up everything.
RUN apk add --no-cache \
            git \
            build-base \
            g++ \
            bash \
            python \
            boost \
            boost-program_options \
            boost-dev

# Clone mapnik and node-mapnik sources.
RUN git clone --progress --recurse-submodules https://github.com/mapnik/mapnik.git /usr/src/mapnik && \
    git clone --progress https://github.com/mapnik/node-mapnik.git /usr/src/node-mapnik

RUN apk add --no-cache \
            freetype \
            freetype-dev \
            icu-libs \
            icu-dev \
            libjpeg \
            libjpeg-turbo-dev \
            libpng \
            libpng-dev \
            libwebp \
            libwebp-dev \
            tiff \
            tiff-dev \
            sqlite \
            sqlite-dev \
            zlib \
            zlib-dev \
            libxml2 \
            libxml2-dev \
            harfbuzz \
            harfbuzz-dev \
            cairo \
            cairo-dev \
            libpq \
            postgresql-dev
RUN apk add --no-cache \
            --repository http://dl-cdn.alpinelinux.org/alpine/edge/testing \
            --repository http://dl-cdn.alpinelinux.org/alpine/edge/main \
            gdal \
            gdal-dev \
            proj \
            proj-dev \
            proj-static

ENV MAPNIK_VERSION a0ea7db1a
ENV MAPNIK_NODE_VERSION v4.3.1

# Build mapnik.
RUN cd /usr/src/mapnik && \
    git reset --hard ${MAPNIK_VERSION} && \
    git submodule update --init && \
    ./configure PREFIX=/opt/mapnik INPUT_PLUGINS='all' CUSTOM_DEFINES='-DACCEPT_USE_OF_DEPRECATED_PROJ_API_H=1' && \
    make -j4 && \
    make install && \
    cp /opt/mapnik/lib/lib* /usr/local/lib/

# Build node-mapnik.
RUN cd /usr/src/node-mapnik && \
    git checkout ${MAPNIK_NODE_VERSION} && \
    sed -i -e "s/var path = require('path');/\"; echo '' > \$\{MODULE_PATH\}\/mapnik_settings.js; echo \"/"\
        -e 's/" > ${MODULE_PATH}\/mapnik_settings\.js/" >> ${MODULE_PATH}\/mapnik_settings.js/' scripts/postinstall.sh && \
    make release_base

RUN cd /usr/src/node-mapnik && \
    mkdir -p /opt/node-mapnik/node_modules/.bin && \
    cp -a bin lib tools package.json package-lock.json README.md /opt/node-mapnik/ && \
    cp -a node_modules/.bin/node-pre-gyp /opt/node-mapnik/node_modules/.bin/ && \
    cp -a node_modules/node-pre-gyp /opt/node-mapnik/node_modules

WORKDIR /opt

# Start shell by default.
CMD ["/bin/sh"]
