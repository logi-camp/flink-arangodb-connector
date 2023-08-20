# Flink ArangoDB Connector

> This repository is under heavy development. Please don't use this in production mode.

# Start to use
## Prerequisite

- Tested with Flink 1.17.
- ArangoDB 3.0 above. The official ArangoDB Java driver supports 3.0 above.
- JDK 11 above.

## Dependency

For Maven users, add the following dependency in your project's pom.xml.

```
<dependency>
	<groupId>top.logicamp.flink_arangodb_connector</groupId>
	<artifactId>flink-arangodb-connector</artifactId>
	<version>1.1-SNAPSHOT</version>
</dependency>
```

## Code

Use ArangoDBSink in your Flink DataStream application.

```java
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	// non-transactional sink with a flush strategy of 1000 documents or 10 seconds
	ArangoDBConnectorOptions options = ArangoDBConnectorOptions.builder()
		.withDatabase("my_db")
		.withCollection("my_collection")
		.withHost("127.0.0.1")
		.withPort("8529")
		.withUseSsl("true")
		.build();

	env.addSource(...)
	.sinkTo(new ArangoDBSink<>(new StringDocumentSerializer(), options));

	env.execute();
```

Use ArangoDBSink in your Flink Table/SQL application.

```java
	TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

	env.executeSql("create table tbl_user_gold (" +
			"    user_id long," +
			"    gold long," +
			"    PRIMARY key(user_id) NOT ENFORCED" +
			") with (" +
			"    'connector'='arangodb'," +
			"    'host' = '127.0.0.1'," +
			"    'port' = '8529'," +
			"    'database' = 'mydb'," +
			"    'collection' = 'user_gold'" +
			")"

	Table userGold = env.executeQuery("select * from tbl_user_gold");
	);
```

# Configuration

Flink ArangoDB Connector can be configured using `ArangoDBConnectorOptions`(recommended) or properties in DataStream API and properties
in Table/SQL API.

## ArangoDBSink

| option                                   | properties key                               | description                                                                                    | default value |
|------------------------------------------|----------------------------------------------|------------------------------------------------------------------------------------------------|--------------|
| ArangoDBConnectorOptions.useSsl          | useSsl                                       | Whether SSL for connection or not (alternative to http:// schema in full url format)           | false        |

# Build from source

Checkout the project, and use Maven to build the project locally.

```
$ mvn verify
```
