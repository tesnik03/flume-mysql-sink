package com.tesnik.flume.sink;

import com.google.common.base.Preconditions;
import org.apache.flume.*;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.conf.Configurable;
 
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by tiwariaa on 2/11/2016.
 */

public class MySqlSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(MySqlSink.class);
    private String password;
    private String user;
    private String driver;
    private String db_url;
    private CounterGroup counterGroup;
    DAOClass daoClass;
    public MySqlSink() {
        counterGroup = new CounterGroup();
        daoClass = new DAOClass();
		
    }

    @Override
    public void configure(Context context) {
        db_url = context.getString("url");
        password = context.getString("password");
        user = context.getString("user");
        driver = context.getString("driver");
        Preconditions.checkState(password != null, "No password specified");
        Preconditions.checkState(user != null, "No user specified");
    }


    @Override
    public void start() {
        logger.info("Mysql sink starting");
        try {
            daoClass.createConnection(driver, db_url, user, password);
        } catch (Exception e) {
            logger.error("Unable to create MySQL client using url:" + db_url + " username:" + user + ". Exception follows.", e);
      /* Try to prevent leaking resources. */
            daoClass.destroyConnection(db_url, user);

      /* FIXME: Mark ourselves as failed. */
            return;
        }
        super.start();
        logger.debug("MySQL sink {} started", this.getName());
    }

    @Override
    public void stop() {
        logger.info("MySQL sink {} stopping", this.getName());
        daoClass.destroyConnection(db_url, user);
        super.stop();
        logger.debug("MySQL sink {} stopped. Metrics:{}", this.getName(), counterGroup);
    }


    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        try {
            transaction.begin();
            daoClass.createConnection(driver, db_url, user, password);
            Event event = channel.take();
            if (event == null) {
                counterGroup.incrementAndGet("event.empty");
                status = Status.BACKOFF;
            } else {
                daoClass.insertData(event);
                counterGroup.incrementAndGet("event.mysql");
            }
            transaction.commit();

        } catch (ChannelException e) {
            transaction.rollback();
            logger.error("Unable to get event from channel. Exception follows.", e);
            status = Status.BACKOFF;
        } catch (Exception e) {
            transaction.rollback();
            logger.error("Unable to communicate with MySQL server. Exception follows.", e);
            status = Status.BACKOFF;
            daoClass.destroyConnection(db_url, user);
        } finally {
            transaction.close();
        }
        return status;
    }
	

}