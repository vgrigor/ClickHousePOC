package ru.td.ch;

import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.sql.Connection;
import java.sql.SQLException;

import static java.lang.Thread.sleep;


public class ClickHouseConnectionTest {

    static String jdbcURL = "jdbc:clickhouse://localhost:8123";

    public static void main( String[] args ) throws SQLException {
        ClickHouseConnectionTest test = new ClickHouseConnectionTest();
        test.testGetSetCatalog();

        System.out.println( "---" );
        System.out.println( "ClickHouseConnectionTest to: " +  jdbcURL + " : PASSED OK!" );
    }

    void waitInfinite(){
        for(;;){
            try {
                sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public void testGetSetCatalog() throws SQLException {

        ClickHouseDataSource dataSource = new ClickHouseDataSource(
                jdbcURL);
                //"jdbc:clickhouse://192.168.99.101:8123");
                //"jdbc:clickhouse://localhost:8123/default?option1=one%20two&option2=y");
        String[] dbNames = new String[]{"get_set_catalog_test1", "get_set_catalog_test2"};
        try {
            ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
/*            assertEquals(connection.getUrl(), "jdbc:clickhouse://localhost:8123/default?option1=one%20two&option2=y");
            assertEquals(connection.getCatalog(), "default");
            assertEquals(connection.getProperties().getDatabase(), "default");*/

            waitInfinite();

            for (String db : dbNames) {
                connection.createStatement().executeUpdate("CREATE DATABASE " + db);
                connection.createStatement().executeUpdate(
                        "CREATE TABLE " + db + ".some_table ENGINE = TinyLog()"
                                + " AS SELECT 'value_" + db + "' AS field");

                System.out.println( "CREATE DATABASE " + db + " PASSED" );
                System.out.println( "CREATE TABLE on " + db + " PASSED" );

            }
        } finally {
            Connection connection = dataSource.getConnection();
            for (String db : dbNames) {
                connection.createStatement().executeUpdate("DROP DATABASE IF EXISTS " + db);
            }
        }
    }

}
