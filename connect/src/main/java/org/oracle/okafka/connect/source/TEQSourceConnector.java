package org.oracle.okafka.connect.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import org.oracle.okafka.connect.common.TEQSourceConnectorConfig;
import org.oracle.okafka.connect.common.utils.AppInfoParser;
import org.oracle.okafka.connect.source.task.TEQSourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * TODO: Review description
 * TEQSourceConnector is a Kafka Connect Connector implementation that watches a ORACLE TEQ and
 * generates tasks to ingest events to kafka broker.
 */
public class TEQSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(TEQSourceConnector.class);

    private String sourceConnectorName;

    private Map<String, String> configProperties;

    private TEQSourceConnectorConfig config;

    /**
     * Get the version of this task. Usually this should be the same as the corresponding {@link Connector} class's version.
     *
     * @return the version, formatted as a String
     */
    @Override public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Returns the Task implementation for this Connector.
     */
    @Override public Class<? extends Task> taskClass() {
        return (Class) TEQSourceTask.class;
    }


    /**
     * Return an unmodifiable view of the Task list. This method allows modules to provide users with “read-only”
     * access to internal list.
     */
    @Override public List<Map<String, String>> taskConfigs(int taskCount) {
        log.trace("[{}] Entry {}.taskConfigs, tasks.max={}", Thread.currentThread().getId(), this.getClass().getName(), taskCount);

        List<Map<String, String>> taskList = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            Map<String, String> taskSettings = new HashMap<>(this.configProperties);
            taskSettings.put(TEQSourceConnectorConfig.KAFKA_CONNECT_TASK_ID, Integer.toString(i));
            taskList.add(taskSettings);
        }
        return Collections.unmodifiableList(taskList);
    }


    @Override public void start(Map<String, String> originalProps) {
        log.info("[{}] Starting Oracle TEQ Source Connector", Thread.currentThread().getId());

        try {
            this.sourceConnectorName = originalProps.get(TEQSourceConnectorConfig.KAFKA_CONNECT_NAME);
            this.configProperties = originalProps;
            config = new TEQSourceConnectorConfig(originalProps);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override public void stop() {
        log.info("[{}] Stopping Oracle TEQ Source Connector", Thread.currentThread().getId());
    }

    /**
     * Define the configuration for the connector.
     *
     * @return The ConfigDef for this connector.
     */
    @Override public ConfigDef config() {
        return config.getConfig();
    }

    public String toString() {
        return this.sourceConnectorName;
    }


}
