package top.logicamp.arangodb_flink_connector.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import top.logicamp.arangodb_flink_connector.config.MongoConnectorOptions;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class MongoDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final ConfigOption<String> CONNECT_STRING =
            ConfigOptions.key("connect_string")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the mongo connect string");
    private static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the mongo database name");
    private static final ConfigOption<String> COLLECTION =
            ConfigOptions.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the mongo collection name");
    private static final ConfigOption<Boolean> TRANSACTION_ENABLE =
            ConfigOptions.key("sink.transaction.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("whether to enable transaction sink");
    private static final ConfigOption<Boolean> FLUSH_ON_CHECKPOINT =
            ConfigOptions.key("sink.flush.on-checkpoint")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("whether to flush on checkpoints");
    private static final ConfigOption<Integer> FLUSH_SIZE =
            ConfigOptions.key("sink.flush.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("flush size");
    private static final ConfigOption<Duration> FLUSH_INTERVAL =
            ConfigOptions.key("sink.flush.interval")
                    .durationType()
                    .defaultValue(Duration.of(30_000L, ChronoUnit.MILLIS))
                    .withDescription("flush interval");

    private static final ConfigOption<Integer> MAX_IN_FIGHT_FLUSHES =
            ConfigOptions.key("sink.max.in-flight.flushes")
                    .intType()
                    .defaultValue(5)
                    .withDescription("max in-flight flushes before blocking further writes");

    private static final String IDENTIFIER = "mongo";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig options = helper.getOptions();
        helper.validate();
        String connectString = options.get(CONNECT_STRING);
        String database = options.get(DATABASE);
        String collection = options.get(COLLECTION);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        return new MongoDynamicTableSource(connectString, database, collection, physicalSchema);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig options = helper.getOptions();
        helper.validate();
        validate(options);
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        Optional<UniqueConstraint> primaryKey = resolvedSchema.getPrimaryKey();
        List<String> primaryKeyFieldNames;
        if (primaryKey.isPresent()) {
            primaryKeyFieldNames = primaryKey.get().getColumns();
        } else {
            primaryKeyFieldNames = Collections.emptyList();
        }
        MongoConnectorOptions mongoSinkOptions = getMongoSinkOptions(options, primaryKeyFieldNames);
        return new MongoDynamicTableSink(mongoSinkOptions, resolvedSchema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CONNECT_STRING);
        options.add(DATABASE);
        options.add(COLLECTION);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        HashSet<ConfigOption<?>> options = new HashSet<>();
        options.add(TRANSACTION_ENABLE);
        options.add(FLUSH_ON_CHECKPOINT);
        options.add(FLUSH_SIZE);
        options.add(FLUSH_INTERVAL);
        options.add(MAX_IN_FIGHT_FLUSHES);
        return options;
    }

    private void validate(ReadableConfig options) {
        // validate config
        if (options.get(TRANSACTION_ENABLE)) {
            Preconditions.checkArgument(
                    options.get(FLUSH_ON_CHECKPOINT),
                    "`%s` must be true when the transactional sink is enabled",
                    FLUSH_ON_CHECKPOINT.key());
        }
        Preconditions.checkArgument(
                options.get(FLUSH_SIZE) > 0, "`%s` must be greater than 0", FLUSH_SIZE.key());
    }

    private MongoConnectorOptions getMongoSinkOptions(
            ReadableConfig config, List<String> primaryKey) {
        return MongoConnectorOptions.builder()
                .withConnectString(config.get(CONNECT_STRING))
                .withDatabase(config.get(DATABASE))
                .withCollection(config.get(COLLECTION))
                .withTransactionEnable(config.get(TRANSACTION_ENABLE))
                .withFlushOnCheckpoint(config.get(FLUSH_ON_CHECKPOINT))
                .withFlushSize(config.get(FLUSH_SIZE))
                .withFlushInterval(config.get(FLUSH_INTERVAL))
                .withUpsertEnable(!primaryKey.isEmpty())
                .withUpsertKey(primaryKey.toArray(new String[0]))
                .build();
    }
}
