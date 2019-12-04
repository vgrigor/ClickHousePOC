package ru.td.ch.repository.dictionary;

import ru.td.ch.repository.Addresses;
import ru.td.ch.repository.ClickHouseTable;

import java.sql.SQLException;

public class AddressRegion extends ClickHouseTable {

    private static String SQLTableCreate =
            "CREATE DICTIONARY somedict (\n" +
            "    id UInt64,\n" +
            "    first Date,\n" +
            "    last Date\n" +
            ")\n" +
            "PRIMARY KEY id\n" +
            "LAYOUT(RANGE_HASHED())\n" +
            "RANGE(MIN first MAX last)";

    public AddressRegion setUp() throws SQLException {

        connection = GetConnection();
        return this;
    }

    public AddressRegion runTest() throws SQLException {
        CreateTable();


        return this;

    }

    public void CreateTable() throws SQLException {
        dropTable("AddressRegion");
        //executeSingleSQL(SQLTableCreate);
        executeSQL(SQLTableCreate);

    }
}
