FROM gradle:jdk11-alpine as build
WORKDIR /app
COPY . .

RUN gradle build

FROM flink:java11
COPY --from=build /app/build/libs/flink-arangodb-connector-*.jar /opt/flink/lib
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar
