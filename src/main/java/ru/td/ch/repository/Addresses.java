package ru.td.ch.repository;

import ru.td.ch.config.CmdlArgs;
import ru.td.ch.metering.Meter;
import ru.td.ch.util.Application;
import ru.td.ch.util.Timer;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;


public class Addresses extends ClickHouseTable{

    static boolean isUpdate = false;
    static boolean isDelete = false;

    public Addresses setUp() throws SQLException {

        connection = GetConnection();

        if(CmdlArgs.instance.isDelete() == false)
            CreateTable();

        return this;
    }

    public static void doLoadData() throws SQLException {
        long dataSize = CmdlArgs.instance.getDataSize();

        Addresses a = new Addresses().setUp();

        a.createChLoadMeter();

        Timer t = Timer.instance().start();
        Timer tWhole = Timer.instance().start();
        int threads = CmdlArgs.instance.getThreads();

        isUpdate = CmdlArgs.instance.isUpdate();
        isDelete = CmdlArgs.instance.isDelete();


        if(threads == 0) { //ONLY TEST - without threads
            a.GenerateLoadStream(dataSize,0);
        }
        else
            {

            CompletableFuture[] features = new CompletableFuture[threads];


                // LOAD
                if( isUpdate == false && isDelete == false) {

                    runAndWait(threads, a, LoadThreadedRunner.class);

                    t.end("LoadThreadedRunner");
                }
                // DELETE
                else
                if( isUpdate == false  && isDelete == true) {

                    //a.set_optimize_throw_if_noop();

                        //a.printStep("DeleteThreadedRunner");
                    runAndWait(threads, a, DeleteThreadedRunner.class);

                    t.end("DeleteThreadedRunner");
                    //Application.wait(1000);

                    a.doOptimize();

                    t.end("doOptimize");
                }
                // UPDATE
                else
                if( isUpdate == true &&  isDelete == false) {

                        //a.printStep("LoadThreadedRunner");
                    a.printSelectCount("Addresses");
                    runAndWait(threads, a, LoadThreadedRunner.class);

                    t.end("LoadThreadedRunner");

                    a.printSelectCount("Addresses");
                        //a.printStep("DeleteThreadedRunner");
                    // 1. DELETE
                    runAndWait(threads, a, DeleteThreadedRunner.class);

                    t.end("DeleteThreadedRunner");

                    // 2. OPTIMIZE - Collapse
                    a.doOptimize();
                    //Application.wait(1000);

                    t.end("doOptimize");

                    // 3. INSERT again
                    //a.printStep("LoadThreadedRunner");
                    runAndWait(threads, a, LoadThreadedRunner.class);
                    a.printSelectCount("Addresses");

                    t.end("LoadThreadedRunner");

                    }
                else{
                    throw new RuntimeException("isUpdate == true &&  isDelete == true : can not be");

                }

        }
        System.out.println("=================================");
        System.out.println("ВСЕ ОТПРАВЛЕНО" + "\tThreads: " + threads + "\tDataSize:"+dataSize);

        tWhole.end("WHOLE WORK");
        System.out.println("=================================");
        System.out.println("");

    }

    static void runAndWait(int nThreads, Addresses a, Class c){


        CompletableFuture[] features1 = new CompletableFuture[nThreads];
        for(int i= 0; i < nThreads; i++) {
            ThreadedRunner runner = createInstance(c, a, i);
            CompletableFuture fi =
                    CompletableFuture.runAsync( runner);
            features1[i] = fi;
        }
        CompletableFuture.allOf(features1).join();
    }

    static ThreadedRunner createInstance(Class c, Addresses a, int threadNumber){
        ThreadedRunner threadedRunner;
        try {
            threadedRunner = (ThreadedRunner)c.getConstructor(Addresses.class, Integer.class).newInstance(a,  threadNumber);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return threadedRunner;
    }

    /**
     * Collapse all records wich have different sign
     *
     * @throws SQLException
     */
    void doOptimize( ) throws SQLException {

        System.out.println("OPTIMIZE: " );
        this.connection.createStatement().execute("OPTIMIZE TABLE " + "Addresses" + " final");

        printSelectCount("Addresses");
    }

    /**
     *
     * Work with error:   DB::Exception: There is no session
     *
     * So named this property in ClickHouse
     * @throws SQLException
     */
    void set_optimize_throw_if_noop( ) throws SQLException {

        System.out.println("optimize_throw_if_noop " );
        this.connection.createStatement().execute("SET optimize_throw_if_noop = 1");

    }



    void printSelectCount(String tableName) throws SQLException {
        String SqlCount = "SELECT Count( * ) AS count from " + tableName;

        ResultSet rs = ExecuteAndReturnResult( SqlCount );
        if(rs.next() == false)
            System.out.println("No Values from SQL:" + SqlCount);
        else
            System.out.println("------- Count(*) = " + rs.getInt("count") );

    }
    void printStep(String String) throws SQLException {

            System.out.println("STEP:  " + String.toUpperCase() );

    }


    static class ThreadedRunner implements  Runnable{
        public ThreadedRunner(){};

        public ThreadedRunner(Addresses a, Integer threadNumber){
            this.threadNumber = threadNumber;
            this.a = a;
        }
        public int threadNumber = 0;
        Addresses a;

        @Override
        public void run() {
            a.GenerateLoadStream(threadNumber);
        }
    }

    static class LoadThreadedRunner extends ThreadedRunner{
        public LoadThreadedRunner(Addresses a, Integer threadNumber){
            this.threadNumber = threadNumber;
            this.a = a;
        }
        public int threadNumber = 0;
        Addresses a;

        @Override
        public void run() {
            a.GenerateLoadStream(threadNumber);
        }
    }

    static class UpdateThreadedRunner extends ThreadedRunner{
        public UpdateThreadedRunner(Addresses a, Integer threadNumber){
            this.threadNumber = threadNumber;
            this.a = a;
        }
        public int threadNumber = 0;
        Addresses a;

        @Override
        public void run() {
            a.GenerateLoadStream(threadNumber);
        }
    }

    static class DeleteThreadedRunner extends ThreadedRunner{
        public DeleteThreadedRunner(Addresses a, Integer threadNumber){
            this.threadNumber = threadNumber;
            this.a = a;
        }
        public int threadNumber = 0;
        Addresses a;

        @Override
        public void run() {
            a.GenerateDeleteStream(threadNumber);
            //a.GenerateLoadStream(threadNumber);
        }
    }


    static class Distributed{

        public static  String SQLTableCreate = "    CREATE TABLE Addresses AS default.Addresses_local\n" +
                "            ENGINE = Distributed(perftest_3shards_1replicas, default, Addresses_local, rand());";


/*        local Table  to create DISTRIBUTED  table Addresses on cluster upon it
CREATE TABLE default.Addresses_local (
`ID` UInt64,
 `Country` String,
 `Region` String,
 `District` String,
 `Street` String,
 `Building` String,
 `Room` Int32,
 `Sign` Int8
) ENGINE = CollapsingMergeTree(Sign) ORDER BY ID SETTINGS index_granularity = 8192

*/


/*    CLUSTER CONFIG  for    /etc/clickhouse-server/config.xml
<remote_servers>
    <perftest_3shards_1replicas>
        <shard>
            <replica>
                <host>10.135.156.210</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>10.135.156.212</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>10.135.156.215</host>
                <port>9000</port>
            </replica>
        </shard>
    </perftest_3shards_1replicas>
</remote_servers>

*/

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
                "ENGINE = StripeLog\n";
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
        boolean distributed  = CmdlArgs.instance.isDistributed();

        if(distributed){
            SQLTableCreate = Distributed.SQLTableCreate;
        }
        else
        switch(engine.toLowerCase()){
            case "default"      : break;
            case "mergetree"    : SQLTableCreate = MergeTree.SQLTableCreate;    break;
            case "memory"       : SQLTableCreate = Memory.SQLTableCreate;       break;
            case "tinylog"      : SQLTableCreate = TinyLog.SQLTableCreate;      break;
            case "stripedlog"   : SQLTableCreate = StripedLog.SQLTableCreate;   break;
        }

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
            throw new RuntimeException(e);
        }

        return 0;
    }

    public long GenerateDeleteStream(int threadNumber){
        try {
            GenerateLoadStream(CmdlArgs.instance.getDataSize()/ CmdlArgs.instance.getThreads(), threadNumber, true, false);

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return 0;
    }

    void createChLoadMeter(){
        Meter.addTimerMeter("ClickHouse: Load 1_000_000 records");
    }

    public void  GenerateLoadStream(long lines, int threadNumber) throws SQLException {
        GenerateLoadStream(lines, threadNumber, Addresses.isDelete, Addresses.isUpdate);
    }

     public void  GenerateLoadStream(long lines, int threadNumber, boolean isDelete, boolean isUpdate) throws SQLException {

        final long count = lines;

        connection.createStatement().sendRowBinaryStream(
                "INSERT INTO Addresses (ID,Country,Region,District,Street,Building,Room,Sign)",
                new ClickHouseStreamCallback() {
                    @Override
                    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {

                        Timer t = Timer.instance().start();


                        for (int i = 0; i < count; i++) {
                            long recordId =  i + (threadNumber)*(count+100);

                            stream.writeInt64(recordId);
                            stream.writeString("Country_" + i);
                            stream.writeString("Region_" + i);
                            stream.writeString("District_" + i);
                            stream.writeString("Street_" + i);
                            stream.writeString("Room_" + i);

                            stream.writeInt32(i % 10);

                            if(isDelete == false)
                                stream.writeInt8(1);
                            else
                                stream.writeInt8(-1);

                            if((i+1) %1_000_000 == 0){
                                System.out.println("Added:" + i + " thread: " + Thread.currentThread().getName());

                                System.out.println("threadNumber: " + threadNumber);

                                long mSec = t.end( );
                                Meter.addTimerMeterValue("ClickHouse: Load 1_000_000 records", mSec);
                            }
                        }
                    }
                }

        );

    }


}

