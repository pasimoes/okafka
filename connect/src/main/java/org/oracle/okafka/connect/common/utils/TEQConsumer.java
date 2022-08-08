package org.oracle.okafka.connect.common.utils;

import oracle.AQ.AQException;
import oracle.jms.AQjmsFactory;
import oracle.jms.AQjmsSession;
import oracle.jms.AQjmsTextMessage;
import oracle.jms.AQjmsTopicSubscriber;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class TEQConsumer implements Closeable {
    protected static final Logger log = LoggerFactory.getLogger(TEQConsumer.class);

    private static String username = "TEQUSER";
    private static String password = "Welcome#10racle";
    private static String url = "jdbc:oracle:thin:@//localhost:1521/ORCLPDB1";
    private static String topicName = "MY_TEQ";

    private TopicConnection connection;
    private TopicSession session;
    private AQjmsTopicSubscriber subscriber;

    public static final Schema VALUE_SCHEMA;

    static {
//        VALUE_SCHEMA = SchemaBuilder.struct()
//                .name("io.confluent.connect.jms.Value").doc("This schema is used to store the value of the JMS message.")
//                .field("messageID",
//                        SchemaBuilder.string().doc("This field stores the value of `Message.getJMSMessageID() <http://docs.oracle.com/javaee/6/api/javax/jms/Message.html#getJMSMessageID()>`_.").build())
//                .field("messageType", SchemaBuilder.string().doc("This field stores the type of message that was received. This corresponds to the subinterfaces of `Message <http://docs.oracle.com/javaee/6/api/javax/jms/Message.html>`_. `BytesMessage <http://docs.oracle.com/javaee/6/api/javax/jms/BytesMessage.html>`_ = `bytes`, `MapMessage <http://docs.oracle.com/javaee/6/api/javax/jms/MapMessage.html>`_ = `map`, `ObjectMessage <http://docs.oracle.com/javaee/6/api/javax/jms/ObjectMessage.html>`_ = `object`, `StreamMessage <http://docs.oracle.com/javaee/6/api/javax/jms/StreamMessage.html>`_ = `stream` and `TextMessage <http://docs.oracle.com/javaee/6/api/javax/jms/TextMessage.html>`_ = `text`. The corresponding field will be populated with the values from the respective Message subinterface.").build())
//                .field("timestamp", SchemaBuilder.int64().doc("Data from the `getJMSTimestamp() <http://docs.oracle.com/javaee/6/api/javax/jms/Message.html#getJMSTimestamp()>`_ method.").build())
//                .field("deliveryMode", SchemaBuilder.int32().doc("This field stores the value of `Message.getJMSDeliveryMode() <http://docs.oracle.com/javaee/6/api/javax/jms/Message.html#getJMSDeliveryMode()>`_.").build())
//                .field("correlationID", SchemaBuilder.string().optional().doc("This field stores the value of `Message.getJMSCorrelationID() <http://docs.oracle.com/javaee/6/api/javax/jms/Message.html#getJMSCorrelationID()>`_.").build())
//                .field("replyTo", DESTINATION_SCHEMA)
//                .field("destination", DESTINATION_SCHEMA)
//                .field("redelivered", SchemaBuilder.bool().doc("This field stores the value of `Message.getJMSRedelivered() <http://docs.oracle.com/javaee/6/api/javax/jms/Message.html#getJMSRedelivered()>`_.").build())
//                .field("type", SchemaBuilder.string().optional().doc("This field stores the value of `Message.getJMSType() <http://docs.oracle.com/javaee/6/api/javax/jms/Message.html#getJMSType()>`_.").build())
//                .field("expiration", SchemaBuilder.int64().doc("This field stores the value of `Message.getJMSExpiration() <http://docs.oracle.com/javaee/6/api/javax/jms/Message.html#getJMSExpiration()>`_.").build())
//                .field("priority", SchemaBuilder.int32().doc("This field stores the value of `Message.getJMSPriority() <http://docs.oracle.com/javaee/6/api/javax/jms/Message.html#getJMSPriority()>`_.").build())
//                .field("properties", SchemaBuilder.map(Schema.STRING_SCHEMA, PROPERTY_VALUE_SCHEMA).doc("This field stores the data from all of the properties for the Message indexed by their propertyName.").build())
//                .field("bytes", SchemaBuilder.bytes().optional().doc("This field stores the value from `BytesMessage.html.readBytes(byte[]) <http://docs.oracle.com/javaee/6/api/javax/jms/BytesMessage.html#readBytes(byte[])>`_.").build())
//                .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, PROPERTY_VALUE_SCHEMA).optional().doc("This field stores the data from all of the map entries returned from `MapMessage.getMapNames() <http://docs.oracle.com/javaee/6/api/javax/jms/MapMessage.html#getMapNames()>`_ for the Message indexed by their key.").build())
//                .field("text", SchemaBuilder.string().optional().doc("This field stores the value from `TextMessage.html.getText() <http://docs.oracle.com/javaee/6/api/javax/jms/TextMessage.html#getText()>`_.").build()).build();

        VALUE_SCHEMA = SchemaBuilder.struct()
                .name("org.oracle.okafka.connect.jms.Value").version(1).doc("This schema is used to store the value of the JMS type message.")
                .field("messageID", Schema.STRING_SCHEMA) // Message.getJMSMessageID()
                .field("messageType", Schema.STRING_SCHEMA) // Message.getJMSType()
                .field("timestamp", Schema.INT64_SCHEMA)  // getJMSTimestamp()
                .field("text", Schema.STRING_SCHEMA)
                .build();

    }

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
                //System.out.println(message.getText());
                log.info("{} Received message with id='{}'", this, message.getJMSMessageID());
                log.info("{} Received message with txt='{}'", this, message.getText());

                String messageID = message.getJMSMessageID();
                String messageType = message.getJMSType();

                final Map<String, String> sourcePartition = null;
                final Map<String, String> sourceOffset = null;

                String topic = "connect-test";

//                Struct value = (new Struct(this.VALUE_SCHEMA))
//                        .put("messageID", messageID)
//                        .put("timestamp", Long.valueOf(message.getJMSTimestamp()))
//                        .put("deliveryMode", Integer.valueOf(message.getJMSDeliveryMode()))
//                        .put("correlationID", message.getJMSCorrelationID())
//                        .put("replyTo", destination(message.getJMSReplyTo()))
//                        .put("destination", destination(message.getJMSDestination()))
//                        .put("redelivered", Boolean.valueOf(message.getJMSRedelivered()))
//                        .put("type", message.getJMSType())
//                        .put("expiration", Long.valueOf(message.getJMSExpiration()))
//                        .put("priority", Integer.valueOf(message.getJMSPriority()))
//                        .put("properties", properties);

                Struct value = (new Struct(this.VALUE_SCHEMA))
                        .put("messageID", messageID)
                        .put("messageType", "TEXT")
                        .put("timestamp", Long.valueOf(message.getJMSTimestamp()))
                        .put("text", message.getText());

                log.info("{} Value built='{}'", this, value.toString());

                SourceRecord nr = new SourceRecord(sourcePartition, sourceOffset, topic, this.VALUE_SCHEMA, value);
                return nr;
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
