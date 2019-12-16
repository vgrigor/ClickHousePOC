#Readme

###How to call:

#### Sample call:
java  -jar ClickHousePOC-0.0.1-SNAPSHOT.jar  --threads 16 -ds 100000000 -en memory  -ip 127.0.0.1 --table Addresses
load 100 millions records
by 16 threads
into table 'Addresses'
in 'Memory'  engine 

#### Sample call 2:
java  -jar ClickHousePOC-0.0.1-SNAPSHOT.jar  --threads 2 -ds 1000000 -en memory  -ip 127.0.0.1 --table AddressesFK
load 1 million records
by 2 threads
into table 'AddressesFK'
in 'MergeTree'  engine 




# Call Params

    @Parameter:   "-th", "--threads"
            description = "Number of load data threads",
            Very important!

    @Parameter:   "-ds", "--datasize"
            description = "Load data SIZE",
            Very important!

    @Parameter:   "-en", "--engine"
            description = "Table Engine",
        One of:            
            mergetree (default value)  
            memory     
            tinylog    
            stripedlog 
        Important

   
    @Parameter:   "-ip", "--useIP"
            description = "other computer IP instead of localhost",

    @Parameter:   "-t", "--table"
            description = "load table",
    Possible values:
        "AddressRegion"  Dictionary  table
        "Addresses"	    Main Table
        "AddressesFK"	Main Table with FK fiels - used to join with Dictionary 
    Program knows what to do in database with this params - behavior is encoded
    Important

    @Parameter:   "-rip", "--useRealIp"
            description = "get and use Real Ip unstead of localhost",
    Do not use. It is for some errors corrections


-- Unused
    @Parameter:   "-u", "--update"
            description = "update table Addreses",
-- Unused
    @Parameter:   "-d", "--delete"
            description = "delete from table Addreses",
-- Unused            
    @Parameter:   "-j", "--JOIN"
            description = "generate JOIN table, not fill",            

