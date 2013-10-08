package net.johnewart.gearman.server.config;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import net.johnewart.gearman.engine.queue.factories.MemoryJobQueueFactory;
import net.johnewart.gearman.engine.queue.factories.PostgreSQLPersistedJobQueueFactory;
import net.johnewart.gearman.engine.queue.factories.RedisPersistedJobQueueFactory;
import net.johnewart.gearman.engine.util.JobHandleFactory;
import net.johnewart.gearman.server.util.SnapshottingJobQueueMonitor;
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

public class GearmanServerConfiguration implements ServerConfiguration {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(GearmanServerConfiguration.class);

    public static final int DEFAULT_PORT = 4730;
    public static final int DEFAULT_HTTP_PORT = 8080;

    private final int port;
    private final int httpPort;
    private final boolean enableSSL;
    private final boolean debugging;
    private final String hostName;
    private final JobQueueFactory jobQueueFactory;
    private final JobManager jobManager;
    private final SnapshottingJobQueueMonitor jobQueueMonitor;

    private GearmanServerConfiguration() {
        port = DEFAULT_PORT;
        httpPort = DEFAULT_HTTP_PORT;
        enableSSL = false;
        debugging = false;
        hostName = "localhost";
        jobQueueFactory = new MemoryJobQueueFactory();
        jobManager = new JobManager(getJobQueueFactory());
        jobQueueMonitor = new SnapshottingJobQueueMonitor(getJobManager());
        JobHandleFactory.setHostName(getHostName());
    }

    public GearmanServerConfiguration(File configFile) {
        // Do stuff...
        this();
    }

    public GearmanServerConfiguration(String... args) throws ParseException {
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

                LOG.info("Hostname: " + getHostName());

                Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
                if(isDebugging())
                {
                    // Log debug data
                    root.setLevel(Level.DEBUG);
                } else {
                    // Errors only, please
                    root.setLevel(Level.ERROR);
                }

                jobQueueFactory = buildJobQueueFactory(cmd);
                jobManager = new JobManager(getJobQueueFactory());
                jobQueueMonitor = new SnapshottingJobQueueMonitor(getJobManager());
                JobHandleFactory.setHostName(getHostName());
            }
        } catch (ParseException | IllegalArgumentException e) {
            formatter.printHelp("java -jar gearman-server.jar", options);
            throw e;
        }
    }

    private static JobQueueFactory buildJobQueueFactory(final CommandLine cmd) {
        final JobQueueFactory jobQueueFactory;
        final String storageName = cmd.getOptionValue("storage");

        if(storageName == null)
        {
            jobQueueFactory = new MemoryJobQueueFactory();
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

                    jobQueueFactory = new PostgreSQLPersistedJobQueueFactory(pghost, pgport, pgdbname, pguser, pgpass);
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

                    jobQueueFactory = new RedisPersistedJobQueueFactory(redisHostname, redisPort);
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported storage engine name provided: " + storageName);
            }
        }

        return jobQueueFactory;
    }

    private Options buildOptions() {
        final Options options = new Options();

        options.addOption(null, "port", true, "Port to listen on (default: 4730)");
        options.addOption(null, "storage", true, "Storage engine to use (redis, postgresql), default is memory only");
        options.addOption(null, "web-port", true, "Port for the HTTP service (default: 8080)");
        options.addOption(null, "hostname", true, "Hostname to use (default inferred from local hostname)");


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

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public int getHttpPort() {
        return httpPort;
    }

    @Override
    public boolean isSSLEnabled() {
        return enableSSL;
    }

    @Override
    public boolean isDebugging() {
        return debugging;
    }

    @Override
    public String getHostName() {
        return hostName;
    }

    @Override
    public JobQueueFactory getJobQueueFactory() {
        return jobQueueFactory;
    }

    @Override
    public JobManager getJobManager() {
        return jobManager;
    }

    @Override
    public SnapshottingJobQueueMonitor getJobQueueMonitor() {
        return jobQueueMonitor;
    }
}
