package ru.td.ch.repository;

import ru.td.ch.config.CmdlArgs;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ClickHouseTable {

    static String jdbcURL = "jdbc:clickhouse://" + getIP() +":8123";

    ClickHouseDataSource dataSource = new ClickHouseDataSource( jdbcURL);
    protected ClickHouseConnection connection;

    static public String getIP()  {
        InetAddress inetAddress = null;
        String IP = "localhost";

        boolean  isUseRealIp  = CmdlArgs.instance.isUseRealIp();

        //For some servers works only by real IP, but it was bad servers - not used this days
        if(isUseRealIp)
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

    public ResultSet ExecuteAndReturnResult(String SQL) throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery(SQL);

        return rs;
    }
}
