package org.oracle.okafka.connect.source.task;

import oracle.AQ.AQException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.oracle.okafka.connect.common.TEQSourceConnectorConfig;
import org.oracle.okafka.connect.common.utils.AppInfoParser;

import org.oracle.okafka.connect.common.utils.TEQConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class TEQSourceTask extends SourceTask {
    static final Logger log = LoggerFactory.getLogger(TEQSourceTask.class);

    private TEQConsumer consumer = new TEQConsumer();
    private String connectorName;
    private String taskId;
    private final Time clock = Time.SYSTEM;


    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        this.connectorName = properties.get(TEQSourceConnectorConfig.KAFKA_CONNECT_NAME);
        this.taskId = properties.get(TEQSourceConnectorConfig.KAFKA_CONNECT_TASK_ID);

        log.info("[{}] Starting Oracle TEQ Connect [{}] Source Task [{}]", Thread.currentThread().getId(), this.connectorName, this.taskId);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        //TODO write the task number
        log.info("[{}] Starting Oracle TEQ Connect Task [{}] poll.", Thread.currentThread().getId(), this.taskId);
        //TODO write code for retry/pending commit

        List<SourceRecord> records = null;

        receive();
        //JmsSourceRecord record = (JmsSourceRecord)this.retryPolicy.call("receive JMS message", () -> receive(this.duration));

        return null;
    }

    protected SourceRecord receive() {
        log.info("[{}] receiving TEQ Messages.", Thread.currentThread().getId());
        try {
            this.consumer.connect();
            this.consumer.receive();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (JMSException e) {
            throw new RuntimeException(e);
        } catch (AQException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public void stop() {

    }
}
