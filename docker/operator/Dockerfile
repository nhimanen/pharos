# syntax=docker/dockerfile:1
# The pharos-operator image: Temurin 25 JRE and the operator jar. It contains no pharos fat jar.
# Build from the repository root:  docker build -f docker/operator/Dockerfile -t pharos-operator:<version> .

FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /build
COPY pom.xml ./
COPY pharos-api/pom.xml pharos-api/
COPY app/pom.xml app/
COPY operator/pom.xml operator/
COPY pharos-api/src pharos-api/src
COPY operator/src operator/src
# The app module is not built (-pl operator -am builds pharos-api and the operator only); its pom is
# only needed for the reactor. The glob matches the shaded jar, not original-pharos-operator-*.jar.
RUN mvn -B -q -pl operator -am -DskipTests package \
    && cp operator/target/pharos-operator-*.jar /build/pharos-operator.jar

FROM eclipse-temurin:25-jre
RUN groupadd --gid 10001 pharos \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin pharos
COPY --from=build /build/pharos-operator.jar /opt/pharos-operator/pharos-operator.jar
USER 10001:10001
# Container-aware heap; the operator is small.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "-jar", "/opt/pharos-operator/pharos-operator.jar"]
