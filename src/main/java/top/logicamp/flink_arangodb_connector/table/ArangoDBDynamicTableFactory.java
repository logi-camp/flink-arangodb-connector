package top.logicamp.flink_arangodb_connector.table;

import top.logicamp.flink_arangodb_connector.config.ArangoDBConnectorOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class ArangoDBDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableFactory {

    private static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the arangodb bootstrap host");

    private static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .noDefaultValue()
                    .withDescription("the arangodb bootstrap port");

    private static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the arangodb database name");
    private static final ConfigOption<String> COLLECTION =
            ConfigOptions.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the arangodb collection name");

    private static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the arangodb password");

    private static final ConfigOption<String> USER =
            ConfigOptions.key("user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the arangodb user");

    private static final ConfigOption<Boolean> USE_SSL =
            ConfigOptions.key("use-ssl")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("the arangodb use ssl");

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

    private static final String IDENTIFIER = "arangodb";

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
        ArangoDBConnectorOptions ArangoDBSinkOptions =
                getArangoDBSinkOptions(options, primaryKeyFieldNames);
        return new ArangoDBDynamicTableSink(ArangoDBSinkOptions, resolvedSchema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST);
        options.add(PORT);
        options.add(DATABASE);
        options.add(COLLECTION);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        HashSet<ConfigOption<?>> options = new HashSet<>();
        options.add(PASSWORD);
        options.add(USER);
        options.add(USE_SSL);
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

    private ArangoDBConnectorOptions getArangoDBSinkOptions(
            ReadableConfig config, List<String> primaryKey) {
        return ArangoDBConnectorOptions.builder()
                .withHost(config.get(HOST))
                .withPort(config.get(PORT))
                .withUseSsl(config.get(USE_SSL))
                .withDatabase(config.get(DATABASE))
                .withCollection(config.get(COLLECTION))
                .withPassword(config.get(PASSWORD))
                .withUser(config.get(USER))
                .withTransactionEnable(config.get(TRANSACTION_ENABLE))
                .withFlushOnCheckpoint(config.get(FLUSH_ON_CHECKPOINT))
                .withFlushSize(config.get(FLUSH_SIZE))
                .withFlushInterval(config.get(FLUSH_INTERVAL))
                .withUpsertEnable(!primaryKey.isEmpty())
                .withUpsertKey(primaryKey.toArray(new String[0]))
                .build();
    }
}
