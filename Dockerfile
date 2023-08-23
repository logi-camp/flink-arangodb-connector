FROM gradle:jdk11-alpine as build
WORKDIR /app
COPY . .

RUN gradle build

FROM flink:java11
COPY --from=build /app/build/libs/flink-arangodb-connector-*.jar /opt/flink/lib
