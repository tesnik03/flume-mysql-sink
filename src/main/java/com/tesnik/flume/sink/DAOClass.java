package com.tesnik.flume.sink;

import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by tiwariaa on 4/22/2016.
 */
public class DAOClass {
    private static final Logger logger = LoggerFactory.getLogger(DAOClass.class);
    private static final String INSERT_QUERY = "INSERT INTO messages(message, updated_at, created_at) values (?, NOW(), NOW())";
    private Connection connection;
    public void insertData(Event event) {
        try {
            String body = new String(event.getBody());
            PreparedStatement insertStmnt = connection.prepareStatement(INSERT_QUERY);
            insertStmnt.setString(1, escapeSQL(body));
            insertStmnt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createConnection(String driver, String db_url, String user, String password) throws IOException {
        if (connection == null) {
            try {
                Class.forName(driver);
                connection = DriverManager.getConnection(db_url, user, password);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void destroyConnection(String db_url, String user) {
        if (connection != null) {
            logger.debug("Destroying connection to: {}:{}", db_url, user);
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        connection = null;
    }

    public static String escapeSQL(String s){
        return  s.replaceAll("'", "\\'");
    }

    public Connection getConnection(){
        return connection;
    }
}
