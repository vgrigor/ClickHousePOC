package ru.td.ch.repository.dictionary;

import ru.td.ch.repository.Addresses;
import ru.td.ch.repository.ClickHouseTable;

import java.sql.SQLException;

public class AddressRegion extends ClickHouseTable {

    private static String SQLTableCreate =
            "CREATE DICTIONARY somedict (\n" +
            "    id UInt64,\n" +
            "    first String,\n" +
            "    last String\n" +
            ")\n" +
            "PRIMARY KEY id\n" +
            "LAYOUT(FLAT())\n" +
            ";";
             // "RANGE(MIN first MAX last)";

    static class Memory{
        public static String SQLTableCreate = "CREATE TABLE AddressRegion\n" +
                "(\n" +
                "    ID UInt64,\n" +
                "    Country String,\n" +
                "    Region String,\n" +
                "    District String,\n" +
                "    Street String,\n" +
                "    Building String,\n" +
                "    Room Int32,\n" +
                "    \n" +
                "    Sign Int8\n" +
                ")\n" +
                "ENGINE = Memory\n";
    }

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
