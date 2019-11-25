package ru.td.ch;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 */
public class SimpleQuery {

    public static void main(String[] args) throws Exception {

        //BasicConfigurator.configure();

        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        //Connection connection = DriverManager.getConnection("jdbc:clickhouse://127.0.0.1:9000");
        Connection connection = DriverManager.getConnection("jdbc:clickhouse://192.168.99.101:8123");

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT (number % 3 + 1) as n, sum(number) FROM numbers(10000000) GROUP BY n");

        while (rs.next()) {
            System.out.println(rs.getInt(1) + "\t" + rs.getLong(2));
        }
    }
}
