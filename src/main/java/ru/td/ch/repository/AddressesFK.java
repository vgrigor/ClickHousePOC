package ru.td.ch.repository;

import ru.td.ch.config.CmdlArgs;
import ru.td.ch.metering.Meter;
import ru.td.ch.util.Timer;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;


public class AddressesFK extends ClickHouseTable{

    public AddressesFK setUp() throws SQLException {

        connection = GetConnection();
        CreateTable();
        return this;
    }

    public static void doLoadData() throws SQLException {
        long dataSize = CmdlArgs.instance.getDataSize();

        AddressesFK a = new AddressesFK().setUp();

        //a.createChLoadMeter();

        Timer t = Timer.instance().start();
        int threads = CmdlArgs.instance.getThreads();
        if(threads == 1) {
            a.GenerateLoadStream(dataSize,0);

        }
        else {

            CompletableFuture[] features1 = new CompletableFuture[threads];

            for(int i= 0; i < threads; i++){

                CompletableFuture fi =
                CompletableFuture.runAsync( new ThreadedRunner(a,i)  );

                features1[i] = fi;
            }
            CompletableFuture.allOf(features1).join();

        }
        System.out.println("=================================");
        System.out.println("ВСЕ ОТПРАВЛЕНО" + "\tThreads: " + threads + "\tDataSize:"+dataSize);

        t.end();

    }

    static class ThreadedRunner implements  Runnable{
        public ThreadedRunner(AddressesFK a, int i){
            this.i = i;
            this.a = a;
        }
        public int i = 0;
        AddressesFK a;

        @Override
        public void run() {
            a.GenerateLoadStream(i);
        }
    }



    static class TinyLog{
        public static  String SQLTableCreate = "CREATE TABLE Addresses\n" +
                "(\n" +
                "    ID UInt64,\n" +
                "    FkRegion UInt64,\n" +
                "    Country String,\n" +
                "    Region String,\n" +
                "    District String,\n" +
                "    Street String,\n" +
                "    Building String,\n" +
                "    Room Int32,\n" +
                "    \n" +
                "    Sign Int8\n" +
                ")\n" +
                "ENGINE = TinyLog\n";
    }

    static class StripedLog{
        public static  String SQLTableCreate = "CREATE TABLE Addresses\n" +
                "(\n" +
                "    ID UInt64,\n" +
                "    FkRegion UInt64,\n" +
                "    Country String,\n" +
                "    Region String,\n" +
                "    District String,\n" +
                "    Street String,\n" +
                "    Building String,\n" +
                "    Room Int32,\n" +
                "    \n" +
                "    Sign Int8\n" +
                ")\n" +
                "ENGINE = StripeLog\n";
    }


    static class MergeTree{
        public static String SQLTableCreate = "CREATE TABLE Addresses\n" +
                "(\n" +
                "    ID UInt64,\n" +
                "    FkRegion UInt64,\n" +
                "    Country String,\n" +
                "    Region String,\n" +
                "    District String,\n" +
                "    Street String,\n" +
                "    Building String,\n" +
                "    Room Int32,\n" +
                "    \n" +
                "    Sign Int8\n" +
                ")\n" +
                "ENGINE = CollapsingMergeTree(Sign)\n" +
                "ORDER BY ID;";
    }

    static class Memory{
        public static String SQLTableCreate = "CREATE TABLE Addresses\n" +
                "(\n" +
                "    ID UInt64,\n" +
                "    FkRegion UInt64,\n" +
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


    private String SQLTableCreate = "CREATE TABLE Addresses\n" +
            "(\n" +
            "    ID UInt64,\n" +
            "    FkRegion UInt64,\n" +
            "    Country String ,\n" +
            "    Region String ,\n" +
            "    District String ,\n" +
            "    Street String ,\n" +
            "    Building String ,\n" +
            "    Room Int32,\n" +
            "    \n" +
            "    Sign Int8\n" +
            ")\n" +
            "ENGINE = CollapsingMergeTree(Sign)\n" +
            "ORDER BY ID;";

    private String SQLTableDrop = "DROP TABLE AddressesFK;";


    void  CreateTable() throws SQLException {

        dropTable("AddressesFK");

        String engine  = CmdlArgs.instance.getEngine();

        switch(engine.toLowerCase()){
            case "default"      : break;
            case "mergetree"    : SQLTableCreate = MergeTree.SQLTableCreate;    break;
            case "memory"       : SQLTableCreate = Memory.SQLTableCreate;       break;
            case "tinylog"      : SQLTableCreate = TinyLog.SQLTableCreate;      break;
            case "stripedlog"   : SQLTableCreate = StripedLog.SQLTableCreate;   break;
        }

        executeSQL(SQLTableCreate);
    }

/*    void  DropTable() throws SQLException {

        executeSingleSQL(SQLTableDrop);
    }*/

    public long GenerateLoadStream(int threadNumber){
        try {
            GenerateLoadStream(CmdlArgs.instance.getDataSize()/ CmdlArgs.instance.getThreads(), threadNumber);

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return 0;
    }

/*    void createChLoadMeter(){
        Meter.addTimerMeter("ClickHouse: Load 1_000_000 records");
    }*/

     public void  GenerateLoadStream(long lines, int threadNumber) throws SQLException {

        final long count = lines;

        connection.createStatement().sendRowBinaryStream(
                "INSERT INTO Addresses (ID,Country,Region,District,Street,Building,Room,Sign)",
                new ClickHouseStreamCallback() {
                    @Override
                    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {

                        Timer t = Timer.instance().start();

                        for (int i = 0; i <= count; i++) {
                            stream.writeInt64(i + threadNumber*count);
                            stream.writeInt64(i + threadNumber*count % 10_000);
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


}

