package ru.td.ch.repository.dictionary;


import ru.td.ch.config.CmdlArgs;
import ru.td.ch.metering.Meter;
import ru.td.ch.repository.Addresses;
import ru.td.ch.repository.ClickHouseTable;
import ru.td.ch.util.Timer;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

public class AddressRegion extends ClickHouseTable {


    public static void doLoadData() throws SQLException {
        long dataSize = CmdlArgs.instance.getDataSize();

        AddressRegion a = new AddressRegion().setUp();
        a.CreateTable();

        Timer t = Timer.instance().start();

            a.GenerateLoadStream(dataSize,0);


        System.out.println("=================================");
        System.out.println("ВСЕ ОТПРАВЛЕНО" + "\tThreads: " + 1 + "\tDataSize:"+dataSize);

        t.end();

    }

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


    public void  GenerateLoadStream(long lines, int threadNumber) throws SQLException {

        final long count = lines;

        connection.createStatement().sendRowBinaryStream(
                "INSERT INTO AddressRegion (ID,Country,Region,District,Street,Building,Room,Sign)",
                new ClickHouseStreamCallback() {
                    @Override
                    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {

                        Timer t = Timer.instance().start();

                        for (int i = 0; i <= count; i++) {
                            stream.writeInt64(i + threadNumber*count);
                            //stream.writeInt64(i + threadNumber*count % 10_000);
                            stream.writeString("Country_" + i);
                            stream.writeString("Region_" + i);
                            stream.writeString("District_" + i);
                            stream.writeString("Street_" + i);
                            stream.writeString("Room_" + i);

                            stream.writeInt32(i % 10);
                            stream.writeInt8(1);


                            if(i %1_000_000 == 0){
                                System.out.println("Added:" + i + " thread: " + Thread.currentThread().getName());

                                System.out.println("threadNumber: " + threadNumber);

                                long mSec = t.end();
                                Meter.addTimerMeterValue("ClickHouse: Load 1_000_000 records", mSec);
                            }
                        }
                    }
                }

        );

    }


    public AddressRegion runTest() throws SQLException {
        CreateTable();

        return this;
    }

    public void CreateTable() throws SQLException {
        dropTable("AddressRegion");

        executeSQL(Memory.SQLTableCreate);

    }


    public static void main(String[] args) throws SQLException {
        AddressRegion a = new AddressRegion();
        a.CreateTable();
        a.GenerateLoadStream(10000,1);
    }
}
