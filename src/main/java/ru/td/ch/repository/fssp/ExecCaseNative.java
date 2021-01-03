package ru.td.ch.repository.fssp;

import ru.td.ch.config.CmdlArgs;
import ru.td.ch.metering.Meter;
import ru.td.ch.repository.ClickHouseTableNative;
import ru.td.ch.util.Timer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;


public class ExecCaseNative extends /*ClickHouseTable*/ ClickHouseTableNative {

    static boolean isUpdate = false;
    static boolean isDelete = false;

    public ExecCaseNative setUp() throws SQLException {

        connection = GetConnection();

        if(CmdlArgs.instance.isDelete() == false)
            CreateTable();

        return this;
    }

    public static void doLoadData() throws SQLException {
        long dataSize = CmdlArgs.instance.getDataSize();

        ExecCase a = new ExecCase().setUp();

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

                runAndWait(threads, a, ExecCase.LoadThreadedRunner.class);

                t.end("LoadThreadedRunner");
            }
            // DELETE
            else
            if( isUpdate == false  && isDelete == true) {

                //a.set_optimize_throw_if_noop();

                //a.printStep("DeleteThreadedRunner");
                runAndWait(threads, a, ExecCase.DeleteThreadedRunner.class);

                t.end("DeleteThreadedRunner");
                //Application.wait(1000);

                a.doOptimize();

                t.end("doOptimize");
            }
            // UPDATE
            else
            if( isUpdate == true &&  isDelete == false) {

                //a.printStep("LoadThreadedRunner");
                a.printSelectCount("ExecCase");
                runAndWait(threads, a, ExecCase.LoadThreadedRunner.class);

                t.end("LoadThreadedRunner");

                a.printSelectCount("ExecCase");
                //a.printStep("DeleteThreadedRunner");
                // 1. DELETE
                runAndWait(threads, a, ExecCase.DeleteThreadedRunner.class);

                t.end("DeleteThreadedRunner");

                // 2. OPTIMIZE - Collapse
                a.doOptimize();
                //Application.wait(1000);

                t.end("doOptimize");

                // 3. INSERT again
                //a.printStep("LoadThreadedRunner");
                runAndWait(threads, a, ExecCase.LoadThreadedRunner.class);
                a.printSelectCount("ExecCase");

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

    static void runAndWait(int nThreads, ExecCase a, Class c){


        CompletableFuture[] features1 = new CompletableFuture[nThreads];
        for(int i= 0; i < nThreads; i++) {
            ExecCase.ThreadedRunner runner = createInstance(c, a, i);
            CompletableFuture fi =
                    CompletableFuture.runAsync( runner);
            features1[i] = fi;
        }
        CompletableFuture.allOf(features1).join();
    }

    static ExecCase.ThreadedRunner createInstance(Class c, ExecCase a, int threadNumber){
        ExecCase.ThreadedRunner threadedRunner;
        try {
            threadedRunner = (ExecCase.ThreadedRunner)c.getConstructor(ExecCase.class, Integer.class).newInstance(a,  threadNumber);

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
        this.connection.createStatement().execute("OPTIMIZE TABLE " + "ExecCase" + " final");

        printSelectCount("ExecCase");
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

        public ThreadedRunner(ExecCase a, Integer threadNumber){
            this.threadNumber = threadNumber;
            this.a = a;
        }
        public int threadNumber = 0;
        ExecCase a;

        @Override
        public void run() {
            a.GenerateLoadStream(threadNumber);
        }
    }

    static class LoadThreadedRunner extends ExecCase.ThreadedRunner {
        public LoadThreadedRunner(ExecCase a, Integer threadNumber){
            this.threadNumber = threadNumber;
            this.a = a;
        }
        public int threadNumber = 0;
        ExecCase a;

        @Override
        public void run() {
            a.GenerateLoadStream(threadNumber);
        }
    }

    static class UpdateThreadedRunner extends ExecCase.ThreadedRunner {
        public UpdateThreadedRunner(ExecCase a, Integer threadNumber){
            this.threadNumber = threadNumber;
            this.a = a;
        }
        public int threadNumber = 0;
        ExecCase a;

        @Override
        public void run() {
            a.GenerateLoadStream(threadNumber);
        }
    }

    static class DeleteThreadedRunner extends ExecCase.ThreadedRunner {
        public DeleteThreadedRunner(ExecCase a, Integer threadNumber){
            this.threadNumber = threadNumber;
            this.a = a;
        }
        public int threadNumber = 0;
        ExecCase a;

        @Override
        public void run() {
            a.GenerateDeleteStream(threadNumber);
            //a.GenerateLoadStream(threadNumber);
        }
    }


    /*
    *           "    Country String"    + codec  +",\n" +
                "    Region String"     + codec  +",\n" +
                "    District String"   + codec  +",\n" +
                "    Street String"     + codec  +",\n" +
                "    Building String"   + codec  +",\n" +
                "    Room Int32"        + codec  +",\n" +
    *
    * */

    enum FieldType{
        Int32,
        Int64,
        String
    }

    static class Field{
        public Field(String name,FieldType type,int cardinality, int length){
            this.name = name;
            this.type = type;
            this.cardinality = cardinality;
            this.length = length;
        }
        public Field(String name,FieldType type,int cardinality){
            this(name,type,cardinality, 0);        }

        String name;
        FieldType type;
        int cardinality;
        int length;
        String codec;
    }

    static ArrayList<Field> fields = new ArrayList<Field>();
    static{
        fields.add(new Field("Fed_Okrug", FieldType.Int32,20));
        fields.add(new Field("OSP", FieldType.Int32,2000));
        fields.add(new Field("SPI", FieldType.Int32,2000));
        fields.add(new Field("TypeOfID", FieldType.Int32,5));
        fields.add(new Field("TypeOfOrgan", FieldType.Int32,5));
        fields.add(new Field("ReasonOfExit", FieldType.Int32,5));
        fields.add(new Field("ExecutionEntity", FieldType.Int32,10));
        fields.add(new Field("TypeOfDebtor", FieldType.Int32,5));
        fields.add(new Field("TypeOfClamator", FieldType.Int32,5));
        fields.add(new Field("Time", FieldType.Int64,0));
        fields.add(new Field("TO", FieldType.Int32,200));
        fields.add(new Field("isIP", FieldType.Int32,2));
        fields.add(new Field("isInSearch", FieldType.Int32,2));
        fields.add(new Field("isInInquiry", FieldType.Int32,2));
        fields.add(new Field("isOUPDS", FieldType.Int32,2));
        fields.add(new Field("Division", FieldType.Int32,20_000));
        fields.add(new Field("Delo_Fact", FieldType.Int32,100_000));
    }

    static String generateSqlDefinition(ArrayList<Field> fields){
        String result= "";
        for(Field field : fields){
            result += " " + field.name + " " + field.type.name() + " "    + TableEngine.codec  +",\n";
        }
        return result;
    }

    static String generateInsertClause(ArrayList<Field> fields){
        String result= "";
        for(Field field : fields){
            result += " " + field.name + ", \n";
        }
        return result;
    }

    static String generateArgsClause(ArrayList<Field> fields){
        String result= "";
        for(Field field : fields){
            result += " ?" + ", ";
        }
        return result;
    }


    static class Distributed{

        public static  String SQLTableCreate = "    CREATE TABLE ExecCase AS default.ExecCase_local\n" +
                "            ENGINE = Distributed(perftest_3shards_1replicas, default, ExecCase_local, rand())";


        public static String codecUInt64 = " Codec(Delta, ZSTD) ";
        public static String codec = " Codec(ZSTD(1)) ";

        static{
            String codecArg = CmdlArgs.instance.getCodec();
            if(codecArg != null && codecArg.length() > 0 )
                codec = " Codec(" + codecArg + ") ";
            else    //default
                codec = "";
        }



        public static String SQLTableCreate_local = "CREATE TABLE ExecCase_local\n" +
                "(\n" +
                "    ID UInt64"         + codecUInt64  +",\n" +
                generateSqlDefinition(fields)  +
                "    \n" +
                "    Sign Int8\n" +
                ")\n" +
                "ENGINE = CollapsingMergeTree(Sign)\n" +
                "ORDER BY ID;";



/*        local Table  to create DISTRIBUTED  table ExecCase on cluster upon it
CREATE TABLE default.ExecCase_local (
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

    static class TableEngine{
        static{
            String codecArg = CmdlArgs.instance.getCodec();
            if(codecArg != null && codecArg.length() > 0 )
                codec = codecArg;
        }
        // 3X for String
        //public static String codec = " Codec(Delta, ZSTD) ";
        // 6X
        //public static String codec = " Codec(ZSTD) ";

        public static String codecUInt64 = " Codec(Delta, ZSTD) ";
        public static String codec = " Codec(ZSTD(1)) ";

    }

    static class TinyLog extends ExecCase.TableEngine {
        public static  String SQLTableCreate = "CREATE TABLE ExecCase\n" +
                "(\n" +
                "    ID UInt64,\n" +
                generateSqlDefinition(fields)  +
                "    \n" +
                "    Sign Int8\n" +
                ")\n" +
                "ENGINE = TinyLog\n";
    }

    static class StripedLog{
        public static  String SQLTableCreate = "CREATE TABLE ExecCase\n" +
                "(\n" +
                "    ID UInt64,\n" +
                generateSqlDefinition(fields) +
                "    \n" +
                "    Sign Int8\n" +
                ")\n" +
                "ENGINE = StripeLog\n";
    }


    static class MergeTree extends ExecCase.TableEngine {
        // 3X for String
        //public static String codec = " Codec(Delta, ZSTD) ";
        // 6X
        //public static String codec = " Codec(ZSTD) ";

        public static String codecUInt64 = " Codec(Delta, ZSTD) ";
        public static String codec = " Codec(ZSTD(1)) ";

        static{
            String codecArg = CmdlArgs.instance.getCodec();
            if(codecArg != null && codecArg.length() > 0 )
                codec = " Codec(" + codecArg + ") ";
            else    //default
                codec = "";
        }


        public static String SQLTableCreate = "CREATE TABLE ExecCase\n" +
                "(\n" +
                "    ID UInt64"         + codecUInt64  +",\n" +
                generateSqlDefinition(fields) +
                "    \n" +
                "    Sign Int8\n" +
                ")\n" +
                "ENGINE = CollapsingMergeTree(Sign)\n" +
                "ORDER BY ID;";
    }

    static class Memory{
        public static String SQLTableCreate = "CREATE TABLE ExecCase\n" +
                "(\n" +
                "    ID UInt64,\n" +

                generateSqlDefinition(fields)  +
                "    \n" +
                "    Sign Int8\n" +
                ")\n" +
                "ENGINE = Memory\n";
    }


    private String SQLTableCreate = "CREATE TABLE ExecCase\n" +
            "(\n" +
            "    ID UInt64,\n" +
            generateSqlDefinition(fields)  +
            "    \n" +
            "    Sign Int8\n" +
            ")\n" +
            "ENGINE = CollapsingMergeTree(Sign)\n" +
            "ORDER BY ID;";

    private String SQLTableDrop = "DROP TABLE ExecCase;";


    void  CreateTable() throws SQLException {

        dropTable("ExecCase");

        String engine  = CmdlArgs.instance.getEngine();
        boolean distributed  = CmdlArgs.instance.isDistributed();

        if(distributed){
            SQLTableCreate = ExecCase.Distributed.SQLTableCreate;

            dropTable("ExecCase_local");
            executeSQL(ExecCase.Distributed.SQLTableCreate_local);

            reInitConnection("10.135.156.212");
            dropTable("ExecCase");
            dropTable("ExecCase_local");
            executeSQL(ExecCase.Distributed.SQLTableCreate_local);
            executeSQL(ExecCase.Distributed.SQLTableCreate);

            reInitConnection("10.135.156.215");
            dropTable("ExecCase");
            dropTable("ExecCase_local");
            executeSQL(ExecCase.Distributed.SQLTableCreate_local);
            executeSQL(ExecCase.Distributed.SQLTableCreate);

            reInitConnection("10.135.156.210");

            executeSQL(ExecCase.Distributed.SQLTableCreate);

        }
        else {
            switch (engine.toLowerCase()) {
                case "default":
                    break;
                case "mergetree":
                    SQLTableCreate = ExecCase.MergeTree.SQLTableCreate;
                    break;
                case "memory":
                    SQLTableCreate = ExecCase.Memory.SQLTableCreate;
                    break;
                case "tinylog":
                    SQLTableCreate = ExecCase.TinyLog.SQLTableCreate;
                    break;
                case "stripedlog":
                    SQLTableCreate = ExecCase.StripedLog.SQLTableCreate;
                    break;
            }

            executeSQL(SQLTableCreate);
        }
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
        GenerateLoadStream(lines, threadNumber, ExecCase.isDelete, ExecCase.isUpdate);
    }


    public void  GenerateLoadStream(long lines, int threadNumber, boolean isDelete, boolean isUpdate) throws SQLException {

        final long count = lines;


        PreparedStatement preparedStatement = GetConnection()//connection
                .prepareStatement("INSERT INTO ExecCase VALUES(?, "+generateArgsClause(fields)+"?)");

        Timer t = Timer.instance().start();
        for (int i = 0; i < count; i++) {

            long recordId =  i + (threadNumber)*(count+100);
            preparedStatement.setLong(1, recordId);
/*            preparedStatement.setString(2, "Zhang San" + i);
            preparedStatement.setString(3, "张三" + i);*/

            int j = 2;
            for(;j <=(1+ fields.size());j++) {
                Field field = fields.get(j-2);
                if (field.cardinality != 0) {
                    switch (field.type) {
                        case Int32:
                            preparedStatement.setInt(j, i % field.cardinality); break;
                        case Int64:
                            preparedStatement.setLong(j, i % field.cardinality); break;
                        case String:
                            preparedStatement.setString(j, "" + i % field.cardinality); break;
                    }
                } else {
                    switch (field.type) {
                        case Int32:
                            preparedStatement.setInt(j, i ); break;
                        case Int64:
                            preparedStatement.setLong(j, i ); break;
                        case String:
                            preparedStatement.setString(j, ""+i ); break;

                    }
                }
            }

            if(isDelete == false)
                preparedStatement.setByte(j, Byte.valueOf("1")  );
            else
                preparedStatement.setByte(j, Byte.valueOf("-1")  );

            preparedStatement.addBatch();

            if((i+1) %1_000_000 == 0 /*|| i== count-1*/) {
                //preparedStatement.get
                preparedStatement.executeBatch();
            }

            if((i+1) %1_000_000 == 0){
                System.out.println("Added:" + i + " thread: " + Thread.currentThread().getName());

                System.out.println("threadNumber: " + threadNumber);

                long mSec = t.end( );
                Meter.addTimerMeterValue("ClickHouse: Load 1_000_000 records", mSec);
            }
        }


/*        connection.createStatement().sendRowBinaryStream(
                "INSERT INTO ExecCase (ID,"+generateInsertClause(fields)+"Sign)",
                //"INSERT INTO ExecCase (ID,Country,Region,District,Street,Building,Room,Sign)",
                new ClickHouseStreamCallback() {
                    @Override
                    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {

                        Timer t = Timer.instance().start();


                        for (int i = 0; i < count; i++) {
                            long recordId =  i + (threadNumber)*(count+100);

                            stream.writeInt64(recordId);

                            for(Field field: fields) {
                                if (field.cardinality != 0) {
                                    switch (field.type) {
                                        case Int32:
                                            stream.writeInt32(i % field.cardinality); break;
                                        case Int64:
                                            stream.writeInt64(i % field.cardinality); break;
                                        case String:
                                            stream.writeString("" + i % field.cardinality); break;
                                    }
                                } else {
                                    switch (field.type) {
                                        case Int32:
                                            stream.writeInt32(i); break;
                                        case Int64:
                                            stream.writeInt64(i); break;
                                        case String:
                                            stream.writeString("" + i); break;

                                    }
                                }
                            }



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

        );*/

    }


/*    public void  GenerateLoadStream(long lines, int threadNumber, boolean isDelete, boolean isUpdate) throws SQLException {

        final long count = lines;

        connection.createStatement().sendRowBinaryStream(
                "INSERT INTO ExecCase (ID,"+generateInsertClause(fields)+"Sign)",
                //"INSERT INTO ExecCase (ID,Country,Region,District,Street,Building,Room,Sign)",
                new ClickHouseStreamCallback() {
                    @Override
                    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {

                        Timer t = Timer.instance().start();


                        for (int i = 0; i < count; i++) {
                            long recordId =  i + (threadNumber)*(count+100);

                            stream.writeInt64(recordId);

                            for(Field field: fields) {
                                if (field.cardinality != 0) {
                                    switch (field.type) {
                                        case Int32:
                                            stream.writeInt32(i % field.cardinality); break;
                                        case Int64:
                                            stream.writeInt64(i % field.cardinality); break;
                                        case String:
                                            stream.writeString("" + i % field.cardinality); break;
                                    }
                                } else {
                                    switch (field.type) {
                                        case Int32:
                                            stream.writeInt32(i); break;
                                        case Int64:
                                            stream.writeInt64(i); break;
                                        case String:
                                            stream.writeString("" + i); break;

                                    }
                                }
                            }



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

    }*/


}


