ARG DOCKER_REGISTRY=frdeboffles
ARG DOCKER_IMAGE_VERSION=latest
FROM ${DOCKER_REGISTRY}/node-mongocxx:${DOCKER_IMAGE_VERSION}

# Update environment PATHs to include mapnik.
ENV PATH="/opt/mapnik/bin:${PATH}"

# Install build & run environment, build mapnik and node-mapnik
# to /opt and then clean up everything.
RUN echo 'http://dl-cdn.alpinelinux.org/alpine/v3.9/main' >> /etc/apk/repositories && \
    apk update && \
    apk add --no-cache \
            git \
            build-base \
            g++ \
            bash \
            python \
            boost==1.67.0-r2 && \
    apk add --no-cache \
            boost-program_options==1.67.0-r2 && \
    apk add --no-cache \
            boost-dev==1.67.0-r2


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

#ENV MAPNIK_VERSION v3.0.23
ENV MAPNIK_VERSION 26d3084ea
#ENV MAPNIK_VERSION 3be9ce8fa
#ENV MAPNIK_VERSION 6e40d4bcf
ENV MAPNIK_NODE_VERSION v4.5.4

# Build mapnik.
RUN cd /usr/src/mapnik && \
    #git checkout ${MAPNIK_VERSION} && \
    git reset --hard ${MAPNIK_VERSION} && \
    git submodule update --init && \
    ./configure PREFIX=/opt/mapnik INPUT_PLUGINS='all' CUSTOM_DEFINES='-DACCEPT_USE_OF_DEPRECATED_PROJ_API_H=1' && \
    make -j4 && \
    make install && \
    cp /opt/mapnik/lib/lib* /usr/local/lib/

RUN apk add --no-cache sparsehash

# Build node-mapnik.
RUN cd /usr/src/node-mapnik && \
    npm i node-pre-gyp && \
    npm i nopt && \
    git checkout ${MAPNIK_NODE_VERSION} && \
    sed -i -e "s/var path = require('path');/\"; echo '' > \$\{MODULE_PATH\}\/mapnik_settings.js; echo \"/"\
        -e 's/" > ${MODULE_PATH}\/mapnik_settings\.js/" >> ${MODULE_PATH}\/mapnik_settings.js/' scripts/postinstall.sh && \
    sed -i '1s/^/#include <sstream>\n/' ./src/mapnik_image_from_bytes.cpp && \
    make release_base

RUN cd /usr/src/node-mapnik && \
    mkdir -p /opt/node-mapnik/node_modules/.bin && \
    cp -a bin lib tools package.json package-lock.json README.md /opt/node-mapnik/ && \
    cp -a node_modules/.bin/node-pre-gyp /opt/node-mapnik/node_modules/.bin/ && \
    cp -a node_modules/.bin/nopt /opt/node-mapnik/node_modules/.bin/ && \
    cp -a node_modules/node-pre-gyp /opt/node-mapnik/node_modules && \
    cp -a node_modules/nopt /opt/node-mapnik/node_modules

WORKDIR /opt

# Start shell by default.
CMD ["/bin/sh"]
