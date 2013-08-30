package net.johnewart.gearman.server.config;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import net.johnewart.gearman.server.persistence.MemoryQueue;
import net.johnewart.gearman.server.persistence.PersistenceEngine;
import net.johnewart.gearman.server.persistence.PostgresQueue;
import net.johnewart.gearman.server.persistence.RedisQueue;
import net.johnewart.gearman.server.storage.JobManager;
import net.johnewart.gearman.server.util.JobHandleFactory;
import net.johnewart.gearman.server.util.JobQueueMonitor;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class ServerConfiguration {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ServerConfiguration.class);

    public static final int DEFAULT_PORT = 4730;
    public static final int DEFAULT_HTTP_PORT = 8080;

    public final int port;
    public final int httpPort;
    public final boolean enableSSL;
    public final boolean debugging;
    public final String hostName;
    public final PersistenceEngine storageEngine;
    public final JobManager jobManager;
    public final JobQueueMonitor jobQueueMonitor;

    private ServerConfiguration() {
        port = DEFAULT_PORT;
        httpPort = DEFAULT_HTTP_PORT;
        enableSSL = false;
        debugging = false;
        hostName = "localhost";
        storageEngine = new MemoryQueue();
        jobManager = new JobManager(storageEngine);
        jobQueueMonitor = new JobQueueMonitor(jobManager);
        JobHandleFactory.setHostName(hostName);
    }

    public ServerConfiguration(File configFile) {
        // Do stuff...
        this();
    }

    public ServerConfiguration(String... args) throws ParseException {
        final Options options = buildOptions();
        final HelpFormatter formatter = new HelpFormatter();
        final CommandLineParser parser = new PosixParser();

        try {
            CommandLine cmd = parser.parse(options, args);

            if(cmd.hasOption("h") || cmd.hasOption("help"))
            {
                formatter.printHelp("java -jar gearman-server.jar [options]", options );
                throw new ParseException("Need things other than help");
            } else {

                debugging = cmd.hasOption("debug");
                enableSSL = cmd.hasOption("enable-ssl");

                if(cmd.hasOption("port"))
                {
                    port = Integer.parseInt(cmd.getOptionValue("port"));
                } else {
                    port = DEFAULT_PORT;
                }

                if(cmd.hasOption("web-port"))
                {
                    httpPort = Integer.parseInt(cmd.getOptionValue("web-port"));
                } else {
                    httpPort = DEFAULT_HTTP_PORT;
                }

                String localHostname;
                try {
                    localHostname = InetAddress.getLocalHost().getHostName();
                } catch (UnknownHostException e) {
                    localHostname = "loacalhost";
                }

                if (cmd.hasOption("hostname")) {
                    hostName = cmd.getOptionValue("hostname");
                } else {
                    hostName = localHostname;
                }

                LOG.info("Hostname: " + hostName);

                Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
                if(debugging)
                {
                    // Log debug data
                    root.setLevel(Level.DEBUG);
                } else {
                    // Errors only, please
                    root.setLevel(Level.ERROR);
                }

                storageEngine = buildStorageEngine(cmd);
                jobManager = new JobManager(storageEngine);
                jobQueueMonitor = new JobQueueMonitor(jobManager);
                JobHandleFactory.setHostName(hostName);
            }
        } catch (ParseException | IllegalArgumentException e) {
            formatter.printHelp("java -jar gearman-server.jar", options);
            throw e;
        }
    }

    private static PersistenceEngine buildStorageEngine(final CommandLine cmd) {
        final PersistenceEngine storageEngine;
        final String storageName = cmd.getOptionValue("storage");

        if(storageName == null)
        {
            storageEngine = new MemoryQueue();
        } else {

            switch (storageName)
            {
                case "postgresql":
                    String pghost     = cmd.getOptionValue("postgres-host");
                    String pgdbname   = cmd.getOptionValue("postgres-dbname");
                    String pguser     = cmd.getOptionValue("postgres-user");
                    String pgpass     = cmd.getOptionValue("postgres-pass");
                    int pgport;

                    try {
                        pgport = Integer.parseInt(cmd.getOptionValue("postgres-port"));
                    } catch (NumberFormatException nfe) {
                        pgport = 5432;
                    }

                    // Some sane defaults
                    if(pghost == null)
                        pghost = "localhost";

                    if(pgdbname == null)
                        pgdbname = "gearman";

                    if(pgpass == null)
                        pgpass =  "gearman";

                    if(pguser == null)
                        pguser = "gearman";

                    if(pgport <= 0)
                        pgport = 5432;

                    storageEngine = new PostgresQueue(pghost, pgport, pgdbname, pguser, pgpass);
                    break;

                case "redis":
                    String redisHostname = cmd.getOptionValue("redis-host");

                    if (redisHostname == null)
                        redisHostname = "localhost";

                    int redisPort;

                    try {
                        redisPort = Integer.parseInt(cmd.getOptionValue("redis-port"));
                    } catch (NumberFormatException nfe) {
                        redisPort = 6379;
                    }

                    storageEngine = new RedisQueue(redisHostname, redisPort);
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported storage engine name provided: " + storageName);
            }
        }

        return storageEngine;
    }

    private Options buildOptions() {
        final Options options = new Options();

        options.addOption(null, "port", true, "Port to listen on (default: 4730)");
        options.addOption(null, "storage", true, "Storage engine to use (redis, postgresql), default is memory only");
        options.addOption(null, "web-port", true, "Port for the HTTP service (default: 8080)");
        options.addOption(null, "hostname", true, "Hostname to use (default inferred from local hostname)");
        options.addOption(null, "cluster-config-file", true, "Cluster configuration file");
        options.addOption(null, "cluster-port", true, "Port to listen on (default: 2500)");
        options.addOption(null, "cluster-mode", true, "Cluster mode to use (writethrough, writebehind), default is write-through");
        options.addOption(null, "cluster-seednodes", true, "Comma-separated list of cluster seed nodes in host:port format");


        // PostgreSQL options
        options.addOption(null, "postgres-user", true, "PostgreSQL user");
        options.addOption(null, "postgres-port", true, "PostgreSQL port");
        options.addOption(null, "postgres-pass", true, "PostgreSQL password");
        options.addOption(null, "postgres-host", true, "PostgreSQL hostname");
        options.addOption(null, "postgres-dbname", true, "PostgreSQL database name");

        // TODO: Allow for fine-tuning how much data to keep with monitor

        // Redis options
        options.addOption(null, "redis-host", true, "Redis hostname");
        options.addOption(null, "redis-port", true, "Redis port");

        // SSL configuration
        options.addOption(null, "enable-ssl", false, "Enable SSL");


        // Debug level
        options.addOption(null, "debug", false, "Log debug messages");

        // Help
        options.addOption("h", "help", false, "Display this message");

        return options;
    }
}
