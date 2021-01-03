package ru.td.ch.repository;

import com.github.housepower.jdbc.ClickHouseDriver;
import ru.td.ch.config.CmdlArgs;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.ClickHouseDataSource;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.Enumeration;


public class ClickHouseTableNative {

    static String jdbcURL = "jdbc:clickhouse://" + getIP() +":8123";
    static String jdbcURLNative ="jdbc:clickhouse://127.0.0.1:9000";

    DataSource dataSource = new ClickHouseDataSource( jdbcURLNative /*jdbcURL*/);
    protected Connection connection;


    public void reInitConnection(String Ip) throws SQLException {
        jdbcURL = "jdbc:clickhouse://" + Ip +":8123";

        dataSource = new ClickHouseDataSource( jdbcURL);
        connection= GetConnection();
        System.out.println(">>>>>>>>>>>>> Connected to IP:  " + Ip);
    }

    static public String getIP()  {
        InetAddress inetAddress = null;
        String IP = CmdlArgs.instance.getIP();

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

        System.out.println("Dropped table " + TableName );
    }

    protected Connection GetConnection() throws SQLException{
        resetDriverManager();

        Connection connection = DriverManager.getConnection(jdbcURLNative);
        //Connection connection = (Connection) dataSource.getConnection();
        return connection;
    }




        // this method should be synchronized
    synchronized protected void resetDriverManager() throws SQLException {
        // remove all registered jdbc drivers
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            DriverManager.deregisterDriver(drivers.nextElement());
        }
        DriverManager.registerDriver(new ClickHouseDriver());
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
            //ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();

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

    /**
     *         ResultSet rs = connection.createStatement().executeQuery(
     *                 "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM test.tsv_stream");
     *         Assert.assertTrue(rs.next());
     *         Assert.assertEquals(rs.getInt("cnt"), 2);
     *         Assert.assertEquals(rs.getLong("sum"), 6);
     *         Assert.assertEquals(rs.getLong("uniq"), 1);
     *
     *
     *
     * @param SQL
     * @return
     * @throws SQLException
     */
    public ResultSet ExecuteAndReturnResult(String SQL) throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery(SQL);

        return rs;
    }
}
