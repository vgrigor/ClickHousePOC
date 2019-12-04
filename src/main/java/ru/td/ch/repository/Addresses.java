package ru.td.ch.repository;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import ru.td.ch.config.CmdlArgs;
import ru.td.ch.metering.Meter;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class Addresses extends ClickHouseTable{


    public Addresses setUp() throws SQLException {

        connection = GetConnection();
        return this;
    }

    public Addresses  runTest() throws SQLException {
        CreateTable();


        return this;

    }

    static class Timer{

        private long time0;
         static public Timer instance(){
            return new Timer();
        }

        public  Timer start(){
            time0 = System.nanoTime();
            return this;
        }

        /**
         *
         * @return   Milliseconds
         */
        public  long end(){
            long time1 = System.nanoTime();

            Long mSec = (time1 - time0)/1_000_000;
            time0 = System.nanoTime();
            System.out.println("Time= " + mSec/1_000 +"," + mSec % 1_000 +" seconds");
            return mSec;
        }

    }

    public static void doLoadData() throws SQLException {
        System.out.println("Context Start Event received.");

        long dataSize = CmdlArgs.instance.getDataSize();//1_000_000;

        Addresses a = new Addresses().setUp().runTest();
        //Addresses a = BeanUtil.getBean(Addresses.class).setUp().runTest();

        a.createChLoadMeter();

        Timer t = Timer.instance().start();
        int threads = CmdlArgs.instance.getThreads();
        if(threads == 1) {
            CompletableFuture<Long> f1 = a.GenerateLoadStream(dataSize,0);
            CompletableFuture.allOf(f1).join();
        }
        else {

            CompletableFuture[] features1 = new CompletableFuture[threads];

            for(int i= 0; i < threads; i++){

                CompletableFuture fi =
                CompletableFuture.runAsync( new I(a,i)  );

                features1[i] = fi;
            }
            CompletableFuture.allOf(features1).join();

        }
        System.out.println("=================================");
        System.out.println("ВСЕ ОТПРАВЛЕНО" + "\tThreads: " + threads + "\tDataSize:"+dataSize);

        t.end();

    }

    static class I implements  Runnable{
        public I(Addresses a, int i){
            this.i = i;
            this.a = a;
        }
        public int i = 0;
        Addresses a;

        @Override
        public void run() {
            a.GenerateLoadStream(i);
        }
    }



    static class TinyLog{
        public static  String SQLTableCreate = "CREATE TABLE Addresses\n" +
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
                "ENGINE = TinyLog\n";
    }

    static class StripedLog{
        public static  String SQLTableCreate = "CREATE TABLE Addresses\n" +
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
                "ENGINE = StripeLog\n";  //NOT CHECKED - may be ERROR
    }


    static class MergeTree{
        public static String SQLTableCreate = "CREATE TABLE Addresses\n" +
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
                "ENGINE = CollapsingMergeTree(Sign)\n" +
                "ORDER BY ID;";
    }

    static class Memory{
        public static String SQLTableCreate = "CREATE TABLE Addresses\n" +
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


    private String SQLTableCreate = "CREATE TABLE Addresses\n" +
            "(\n" +
            "    ID UInt64,\n" +
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

    private String SQLTableDrop = "DROP TABLE Addresses;";


    void  CreateTable() throws SQLException {

        dropTable("Addresses");

        String engine  = CmdlArgs.instance.getEngine();

        switch(engine.toLowerCase()){
            case "default"      : break;
            case "mergetree"    : SQLTableCreate = MergeTree.SQLTableCreate;    break;
            case "memory"       : SQLTableCreate = Memory.SQLTableCreate;       break;
            case "tinylog"      : SQLTableCreate = TinyLog.SQLTableCreate;      break;
            case "stripedlog"   : SQLTableCreate = StripedLog.SQLTableCreate;   break;
        }


        //executeSingleSQL(SQLTableCreate);
        executeSQL(SQLTableCreate);
    }

    void  DropTable() throws SQLException {

        executeSingleSQL(SQLTableDrop);
    }

    public long GenerateLoadStream(int threadNumber){
        try {
            GenerateLoadStream(CmdlArgs.instance.getDataSize()/ CmdlArgs.instance.getThreads(), threadNumber);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return 0;
    }

    void createChLoadMeter(){
        Meter.addTimerMeter("ClickHouse: Load 1_000_000 records");
    }

    @Async("threadPoolTaskExecutor")
     public CompletableFuture< Long > GenerateLoadStream(long lines, int threadNumber) throws SQLException {

        final long count = lines;
        final AtomicLong sum = new AtomicLong();

        connection.createStatement().sendRowBinaryStream(
                "INSERT INTO Addresses (ID,Country,Region,District,Street,Building,Room,Sign)",
                new ClickHouseStreamCallback() {
                    @Override
                    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {


                        Timer t = Timer.instance().start();

                        for (int i = 0; i <= count; i++) {
                            stream.writeInt64(i + threadNumber*count);
                            stream.writeString("Country_" + i);
                            stream.writeString("Region_" + i);
                            stream.writeString("District_" + i);
                            stream.writeString("Street_" + i);
                            stream.writeString("Room_" + i);

                            stream.writeInt32(i % 10);
                            stream.writeInt8(1);
                            //sum.addAndGet(i);

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

        return CompletableFuture.completedFuture(0L);
        //ResultSet rs = connection.createStatement().executeQuery("SELECT count() AS cnt, sum(Room) AS sum FROM Addresses");




    }


}

