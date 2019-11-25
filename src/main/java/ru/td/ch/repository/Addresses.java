package ru.td.ch.repository;

import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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
/*      GenerateStream(1_000_000);
        GenerateStream(1_000_000);
        GenerateStream(1_000_000);
        GenerateStream(1_000_000);
        GenerateStream(1_000_000);
        GenerateStream(1_000_000);
        GenerateStream(1_000_000);
        GenerateStream(1_000_000);*/

        return this;

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
     public CompletableFuture< Long > GenerateStream(long lines) throws SQLException {

        final long count = lines;
        final AtomicLong sum = new AtomicLong();

        connection.createStatement().sendRowBinaryStream(
                "INSERT INTO Addresses (ID,Country,Region,District,Street,Building,Room,Sign)",
                new ClickHouseStreamCallback() {
                    @Override
                    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                        for (int i = 0; i < count; i++) {
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

/*        Assert.assertTrue(rs.next());
        assertEquals(rs.getInt("cnt"), count);
        assertEquals(rs.getLong("sum"), sum.get());*/


    }


}
/*
- "1" name="country" displayName="Страна" 
  name_short 
  
- "regions" mask="" "2" name="region" displayName="Регион" 
  name 
  
- "districts" mask="" "3" name="district_ref" displayName="Район_спр" 
  name 
  
- "cities" mask="" "4" name="city_ref" displayName="Город_спр" 
  name 
  
  "villages_council" mask="" "5" name="village_council_ref" displayName="Сельское образование (с/с)_спр"  
  "adm_districts" mask="" "6" name="adm_district_ref" displayName="Административный округ_спр"  
  "settlements" mask="" "7" name="locality_ref" displayName="Населенный пункт_спр"  
  "city_districts" mask="" "8" name="city_district_ref" displayName="Район города_спр"  
  "" mask="" "9" simpleDataType="String" name="microdistrict" displayName="Микрорайон"  
  "militaries" mask="" "10" name="military" displayName="Воинская часть"  
  "streets" mask="" "11" name="street_ref" displayName="Улица_спр"  
  "String" name="house" displayName="Дом"  
  "String" name="case" displayName="Корпус"  
  "String" name="build" displayName="Строение"  
  "String" name="litera" displayName="Литера"  
  "String" name="room" displayName="Помещение"  
  able="true" displayable="true" mainDisplayable="true" name="addr_string" displayName="Адрес одной строкой"  
  "String" name="fias" displayName="Код ФИАС"  
  "String" name="district" displayName="Район"  
  "String" name="city" displayName="Город"  
  "String" name="village_council" displayName="Сельское образование (с/с)"  
  _ref" displayName="ЗАТО_спр"  
  "String" name="zato" displayName="ЗАТО (поселок городского типа)"  
  "String" name="adm_district" displayName="Административный округ"  
  "String" name="locality" displayName="Населённый пункт"  
  "String" name="city_district" displayName="Район города"  
  "String" name="street" displayName="Улица"  
  <arrayAttribute useAttributeNameForDisplay="false" arrayValueType="String" searchable="true" nullable="true" searchMorphologically="false" searchCaseInsensitive="false" lookupEntityType="" mask="" exchangeSeparator="|" "17" name="flats" displayName="Номера квартир"  
  <complexAttribute nestedEntityName="coordinates" minCount="0" subEntityKeyAttribute="" "20" name="coordinates" displayName="Координаты"  
  </entity>



*/