package ru.td.ch.config;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import static java.lang.System.exit;

public class CmdlArgs {

    public static CmdlArgs instance;

    @Parameter(names = {"-t", "--threads"},
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


    @Parameter(names = {"-v", "--verbose"},
            required = false,
            description = "Enable verbose logging")
    private boolean verbose;
    @Parameter(names = {"-f", "--file"},
            description = "Path and name of file to use",
            required = false)
    private String file;
    @Parameter(names = {"-h", "--help"},
            description = "Help/Usage",
            required = false,
            help = true)
    private boolean help;



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
}
