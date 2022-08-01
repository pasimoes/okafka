package org.oracle.okafka.connect.common;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TEQSourceConnectorConfig extends AbstractConfig {
    private static final Logger log = LoggerFactory.getLogger(TEQSourceConnectorConfig.class);

    // TEQ Configuration
    public static final String DATABASE_URL_CONFIG = "db_url"; // jdbc:oracle:thin:@LAB_DB_SVC?TNS_ADMIN=/home/appuser/wallet
    private static final String DATABASE_URL_DOC =
            "JDBC connection URL.\n"
                    + "For example: ``jdbc:oracle:thin:@localhost:1521:orclpdb1``";
    private static final String DATABASE_URL_DISPLAY = "JDBC URL";

    public static final String DATABASE_USER_CONFIG = "db_user";
    private static final String DATABASE_USER_DOC = "JDBC connection user.";
    private static final String DATABASE_USER_DISPLAY = "JDBC User";

    public static final String DATABASE_PASSWORD_CONFIG = "db_password";
    private static final String DATABASE_PASSWORD_DOC = "JDBC connection password.";
    private static final String DATABASE_PASSWORD_DISPLAY = "JDBC Password";

    private static final String TEQ_TOPIC_CONFIG = "teq.topic";
    private static final String TEQ_TOPIC_DOC = "teq.topic";
    private static final String TEQ_TOPIC_DISPLAY = "teq.topic";

    private static final String TEQ_SUBSCRIBER_CONFIG = "teq.subscriber";
    private static final String TEQ_SUBSCRIBER_DOC = "teq.subscriber";
    private static final String TEQ_SUBSCRIBER_DISPLAY = "teq.subscriber";


    // Kafka Configuration
    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String KAFKA_TOPIC_DOC = "The name of the Kafka topic where the connector writes all records that were read from the JMS broker.";
    public static final String KAFKA_TOPIC_DISPLAY = "Target Kafka topic";

    private static final ConfigDef CONFIG = null;


    // Kafka Connect Task
        public static final String KAFKA_CONNECT_NAME = "name";
    public static final String KAFKA_CONNECT_TASK_ID = "source-task.id";

    public final String topic;

    public TEQSourceConnectorConfig(Map<String, String> originals) {
        super(getConfig(), originals);
        this.topic = getString("kafka.topic");
    }

    public TEQSourceConnectorConfig(ConfigDef definition, Map<String, String> originals) {
        super(definition, originals);
        this.topic = getString("kafka.topic");
    }

    public TEQSourceConnectorConfig(ConfigDef definition, Map<String, String> originals, boolean doLog) {
        super(definition, originals, doLog);
        this.topic = getString("kafka.topic");
    }

    public static ConfigDef getConfig() {
        ConfigDef configDef = new ConfigDef();

        int orderInGroup = 0;

        // Database Group Configurations
        String groupName = "Database";

        configDef.define(DATABASE_URL_CONFIG, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.HIGH, DATABASE_URL_DOC,
                groupName, ++orderInGroup, ConfigDef.Width.LONG, DATABASE_URL_DISPLAY);

        configDef.define(DATABASE_USER_CONFIG, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.HIGH, DATABASE_USER_DOC,
                groupName, ++orderInGroup, ConfigDef.Width.MEDIUM, DATABASE_USER_DISPLAY);

        configDef.define(DATABASE_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, "",
                ConfigDef.Importance.HIGH, DATABASE_PASSWORD_DOC,
                groupName, ++orderInGroup, ConfigDef.Width.MEDIUM, DATABASE_PASSWORD_DISPLAY);

        // TEQ Group Configurations
        groupName = "TEQ";
        orderInGroup = 0;

        configDef.define(TEQ_TOPIC_CONFIG, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.HIGH, TEQ_TOPIC_DOC,
                groupName, ++orderInGroup, ConfigDef.Width.MEDIUM, TEQ_TOPIC_DISPLAY);

        configDef.define(TEQ_SUBSCRIBER_CONFIG, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.HIGH, TEQ_SUBSCRIBER_DOC,
                groupName, ++orderInGroup, ConfigDef.Width.MEDIUM, TEQ_SUBSCRIBER_DISPLAY);

        // KAFKA Group Configurations
        groupName = "kafka";
        orderInGroup = 0;

        configDef.define(KAFKA_TOPIC, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.HIGH, KAFKA_TOPIC_DOC,
                groupName, ++orderInGroup, ConfigDef.Width.LONG, KAFKA_TOPIC_DISPLAY);

        return configDef;
    }

    public static void main(String[] args) {
        System.out.println(getConfig().toEnrichedRst());
    }
}
