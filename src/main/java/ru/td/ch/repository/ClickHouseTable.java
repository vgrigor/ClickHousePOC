package ru.td.ch.repository;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;

public class ClickHouseTable {

    //static String jdbcURL = "jdbc:clickhouse://localhost:8123";
    static String ip = "localhost";
    static String jdbcURL = "jdbc:clickhouse://" + getIP() +":8123";
    ClickHouseDataSource dataSource = new ClickHouseDataSource( jdbcURL);

    //private ClickHouseDataSource dataSource;
    protected ClickHouseConnection connection;


    public void ClickHouseTable() throws UnknownHostException {

    }

    static public String getIP()  {
        InetAddress inetAddress = null;
        String IP = "localhost";

        //For some servers works only by real IP, but it was bad servers - not used this days
        if(false)
        try {
            inetAddress = InetAddress.getLocalHost();
            IP = inetAddress.getHostAddress();
        System.out.println("IP Address:- " + IP);

        } catch (UnknownHostException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return IP;
    }

    protected void dropTable(String TableName) throws SQLException {

        GetConnection().createStatement().execute("DROP TABLE IF EXISTS " + TableName);
    }

    protected ClickHouseConnectionImpl GetConnection() throws SQLException{
        ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
        return connection;
    }

    public void executeSingleSQL(String SQL) throws SQLException {

        try {
            ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();

            if(true) {
                connection.createStatement().executeUpdate(SQL);
            }
        } finally {
        }
    }

    public void executeSQL(String SQL)  {

        try {
            ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();

            System.out.println("-----------------------------------------" );
            System.out.println("EXECUTING SQL\n:" + SQL);
            System.out.println("-----------------------------------------" );

            if(true) {
                connection.createStatement().execute(SQL);
            }

        }
        catch(SQLException e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        finally {
        }
    }
}
