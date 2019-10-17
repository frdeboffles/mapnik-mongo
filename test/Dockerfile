ARG DOCKER_REGISTRY=frdeboffles
ARG DOCKER_IMAGE_VERSION=latest
FROM ${DOCKER_REGISTRY}/node-mongo-mapnik:${DOCKER_IMAGE_VERSION}

RUN apk add --update --no-cache mongodb fontconfig ttf-dejavu

RUN npm install mongodb@3.3.3 mapnik@/opt/node-mapnik

COPY init.sh import.js test.js test.xml ./
COPY shp ./shp
CMD ["/bin/sh", "init.sh"]
