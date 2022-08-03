package org.oracle.okafka.connect.common.utils;

import oracle.AQ.AQException;
import oracle.jms.AQjmsFactory;
import oracle.jms.AQjmsSession;
import oracle.jms.AQjmsTextMessage;
import oracle.jms.AQjmsTopicSubscriber;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

public class TEQConsumer implements Closeable {
    protected static final Logger log = LoggerFactory.getLogger(TEQConsumer.class);

    private static String username = "TEQUSER";
    private static String password = "Welcome#10racle";
    private static String url = "jdbc:oracle:thin:@//localhost:1521/ORCLPDB1";
    private static String topicName = "MY_TEQ";

    private TopicConnection connection;
    private TopicSession session;
    private AQjmsTopicSubscriber subscriber;


    public void connect() throws AQException, SQLException, JMSException {
        log.info("[{}] Open Oracle TEQ Connections.", Thread.currentThread().getId());
        // create a topic session
        PoolDataSource ds = PoolDataSourceFactory.getPoolDataSource();
        ds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
        ds.setURL(url);
        ds.setUser(username);
        //ds.setPassword(System.getenv("DB_PASSWORD"));
        ds.setPassword(password);

        // create a JMS topic connection and session
        TopicConnectionFactory tcf = AQjmsFactory.getTopicConnectionFactory(ds);
        this.connection = tcf.createTopicConnection();
        connection.start();
        this.session =
                (AQjmsSession) connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        // create a subscriber on the topic
        Topic topic = ((AQjmsSession) this.session).getTopic(username, topicName);
        this.subscriber =
                (AQjmsTopicSubscriber) this.session.createDurableSubscriber(topic, "my_subscriber");
    }

    public SourceRecord receive() throws JMSException {
        // wait forever for messages to arrive and print them out
        log.info("[{}] Waiting for messages....", Thread.currentThread().getId());

        // the 1_000 is a one second timeout
        AQjmsTextMessage message = (AQjmsTextMessage) this.subscriber.receive(1_000);
        if (message != null) {
            if (message.getText() != null) {
                System.out.println(message.getText());
                log.trace("{} Received message with id='{}'", this, message.getJMSMessageID());
                String messageID = message.getJMSMessageID();
                return new SourceRecord(null, null, this.config.topic, null, this.keySchema, key, this.valueSchema, value, message, messageID, sessionId);

            } else {
                System.out.println();
            }
        }
        session.commit();
        return null;
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        log.info("[{}] Close Oracle TEQ Connections.", Thread.currentThread().getId());
        try {
            this.session.close();
            this.connection.close();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }
}
