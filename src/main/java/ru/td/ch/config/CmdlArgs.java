package ru.td.ch.config;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import static java.lang.System.exit;

public class CmdlArgs {

    public static CmdlArgs instance;


    @Parameter(names = {"-j", "--JOIN"},
            required = false,
            description = "generate JOIN table, not fill",
            help = true)
    private boolean join = false;

    @Parameter(names = {"-th", "--threads"},
            required = false,
            description = "Load data threads",
            help = true)
    private int threads = 1;

    @Parameter(names = {"-ds", "--datasize"},
            required = false,
            description = "Load data SIZE",
            help = true)
    private long dataSize = 100_000_000;

    @Parameter(names = {"-en", "--engine"},
            required = false,
            description = "Table Engine",
            help = true)
    private String engine = "default";

    @Parameter(names = {"-rip", "--useRealIp"},
            description = "useRealIp unstead of localhost",
            required = false,
            help = true)
    private boolean useRealIp = false;


    @Parameter(names = {"-ip", "--useIP"},
            description = "other computer instead of localhost",
            required = false,
            help = true)
    private String IP = "localhost";

    @Parameter(names = {"-t", "--table"},
            description = "load table",
            required = false,
            help = true)
    private String table = "";//"Addresses";



    public static CmdlArgs setup(String[] args) {

        instance = new CmdlArgs();
        JCommander commander
                = JCommander.newBuilder()
                .programName("JCommander Demonstration")
                .addObject(instance)
                .build();

        try {
            commander.parse(args);
        } catch (ParameterException pe) {
            System.err.println(pe.getMessage());
            commander.usage();

            exit(0);

        }
        return instance;
    }


    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public long getDataSize() {
        return dataSize;
    }

    public String getEngine() {
        return engine;
    }

    public boolean isUseRealIp() {
        return useRealIp;
    }

    public boolean isJoin() {
        return join;
    }

    public String getIP() {
        return IP;
    }

    public String getTable() {
        return table;
    }
}
