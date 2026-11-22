# syntax=docker/dockerfile:1
# The pharos image the operator deploys as writers, readers and the router.
# Build from the repository root:  docker build -f docker/pharos/Dockerfile -t pharos:<version> .

FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /build
COPY pom.xml ./
COPY pharos-api/pom.xml pharos-api/
COPY app/pom.xml app/
COPY operator/pom.xml operator/
COPY pharos-api/src pharos-api/src
COPY src src
# The operator module is not part of this image; its pom is only needed for the reactor.
RUN mvn -B -q -pl app -am -DskipTests package \
    && cp target/pharos-*.jar /build/pharos.jar \
    && rm -f /build/target/original-*.jar

FROM eclipse-temurin:25-jre
# python3 + python-hcl2 (Terraform), node (JS), git (writers clone projects)
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip nodejs git ca-certificates \
    && pip3 install --no-cache-dir --break-system-packages python-hcl2 \
    && apt-get purge -y python3-pip \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 10001 pharos \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin pharos \
    && mkdir -p /data/pharos /data/work /etc/pharos \
    && chown -R 10001:10001 /data

COPY --from=build /build/pharos.jar /opt/pharos/pharos.jar
COPY docker/pharos/entrypoint.sh /usr/local/bin/pharos-entrypoint
RUN chmod 0755 /usr/local/bin/pharos-entrypoint

# PHAROS_HOME (model cache, indexes) belongs on the PVC; /etc/pharos/config.json is a ConfigMap mount.
ENV PHAROS_HOME=/data/pharos \
    PHAROS_ROLE=all
USER 10001:10001
WORKDIR /data
VOLUME ["/data"]
EXPOSE 7070
# No HEALTHCHECK: Kubernetes probes (/health) are used instead.
ENTRYPOINT ["/usr/local/bin/pharos-entrypoint"]
