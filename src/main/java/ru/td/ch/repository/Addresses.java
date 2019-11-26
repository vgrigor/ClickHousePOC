package ru.td.ch.repository;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class Addresses extends ClickHouseTable{

    private ClickHouseDataSource dataSource;
    private ClickHouseConnection connection;


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
        public  void end(){
            long time1 = System.nanoTime();

            Long mSec = (time1 - time0)/1_000_000;
            System.out.println("Time= " + mSec/1_000 +"," + mSec % 1_000 +" seconds");
        }

    }

    public static void doLoadData() throws SQLException {
        System.out.println("Context Start Event received.");


        Addresses a = new Addresses().setUp().runTest();
        //Addresses a = BeanUtil.getBean(Addresses.class).setUp().runTest();

        Timer t = Timer.instance().start();


        CompletableFuture< Long > f1 = a.GenerateLoadStream(10_000_000);
        //CompletableFuture< Long > f2 = a.GenerateLoadStream(50_000_000);
        //CompletableFuture< Long > f3 = a.GenerateLoadStream(4_000_000);

        //CompletableFuture.allOf(f1, f2, f3).join();
        //CompletableFuture.allOf(f1, f2).join();
        CompletableFuture.allOf(f1).join();
        System.out.println("=================================");
        System.out.println("ВСЕ ОТПРАВЛЕНО");

        t.end();

    }


    class case_TinyLog{
        private String SQLTableCreate = "CREATE TABLE Addresses\n" +
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


    class case_MergeTree{
        private String SQLTableCreate = "CREATE TABLE Addresses\n" +
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

    class caseMemory{
        private String SQLTableCreate = "CREATE TABLE Addresses\n" +
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
            //"ENGINE = TinyLog\n" +
            //"ENGINE = Memory\n" +
            "ORDER BY ID;";
            //";";

    private String SQLTableDrop = "DROP TABLE Addresses;";


    void  CreateTable() throws SQLException {

        dropTable("Addresses");
        //executeSingleSQL(SQLTableCreate);
        executeSQL(SQLTableCreate);
    }

    void  DropTable() throws SQLException {

        executeSingleSQL(SQLTableDrop);
    }

    @Async("threadPoolTaskExecutor")
     public CompletableFuture< Long > GenerateLoadStream(long lines) throws SQLException {

        final long count = lines;
        final AtomicLong sum = new AtomicLong();

        connection.createStatement().sendRowBinaryStream(
                "INSERT INTO Addresses (ID,Country,Region,District,Street,Building,Room,Sign)",
                new ClickHouseStreamCallback() {
                    @Override
                    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                        for (int i = 0; i <= count; i++) {
                            stream.writeInt64(i);
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
                            }
                        }
                    }
                }

        );

        return CompletableFuture.completedFuture(0L);
        //ResultSet rs = connection.createStatement().executeQuery("SELECT count() AS cnt, sum(Room) AS sum FROM Addresses");




    }


}

