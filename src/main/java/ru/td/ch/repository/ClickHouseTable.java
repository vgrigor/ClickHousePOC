package ru.td.ch.repository;

import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class ClickHouseTable {

    static String jdbcURL = "jdbc:clickhouse://localhost:8123";
    ClickHouseDataSource dataSource = new ClickHouseDataSource( jdbcURL);

    void dropTable(String TableName) throws SQLException {

        GetConnection().createStatement().execute("DROP TABLE IF EXISTS " + TableName);
    }

    ClickHouseConnectionImpl GetConnection() throws SQLException{
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

    public void executeSQL(String SQL) throws SQLException {

        try {
            ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();

            if(true) {
                connection.createStatement().execute(SQL);
            }
        } finally {
        }
    }
}
