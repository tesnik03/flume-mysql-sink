package com.tesnik.flume.sink;

import com.mysql.jdbc.Statement;
import org.apache.flume.*;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

public class MySqlSinkTest {
    private Context context;
    private static final String testMessage = "Test message 1";
    private MySqlSink mySqlSink;
    private static final String url = "jdbc:mysql://localhost:3306/message_container";
    private static final String user = "root";
    private static final String password = "root";
    private static final String driver = "com.mysql.jdbc.Driver";

    @Before
    public void setUp() {
        context = new Context();
        context.put("url", url);
        context.put("user", user);
        context.put("password", password);
        context.put("driver", driver);
        mySqlSink = new MySqlSink();
        mySqlSink.setName("Mysql sink");
        mySqlSink.configure(context);
    }

    @Test
    public void testMessageAdded() {
        Channel memoryChannel = new MemoryChannel();
        Configurables.configure(memoryChannel, context);
        mySqlSink.setChannel(memoryChannel);
        mySqlSink.start();
        Transaction txn = memoryChannel.getTransaction();
        txn.begin();
        Event event = EventBuilder.withBody(testMessage.getBytes());
        memoryChannel.put(event);
        txn.commit();
        txn.close();
        try {
            Sink.Status status = mySqlSink.process();
            if (status == Sink.Status.BACKOFF) {
                fail("Error occured");
            }
        } catch (EventDeliveryException eDelExcp) {
            // noop
        }

        getLatestMessage();

    }

    public String getLatestMessage() {
        DAOClass daoClass = new DAOClass();
        String value = null;
        try {
            daoClass.createConnection(driver, url, user, password);
            java.sql.Connection connection = daoClass.getConnection();
            String sqlString = "SELECT message FROM MESSAGES ORDER BY created_at desc limit 1";
            java.sql.Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(sqlString);

            while (rs.next()) {
                value = rs.getString(1);
            }

            assertEquals("Values inserted not matching ", testMessage, value);
            daoClass.destroyConnection(url, user);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return value;
    }


}