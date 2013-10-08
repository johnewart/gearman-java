package net.johnewart.gearman.cluster.persistence;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.queue.persistence.PersistenceEngine;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;

import static java.lang.String.format;

public class HBasePersistenceEngine implements PersistenceEngine {
    private static Logger LOG = LoggerFactory.getLogger(HBasePersistenceEngine.class);
    private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
    private static final String TABLE_PREFIX = "func_";

    private final HConnection hBaseConnection;
    private final Configuration hbaseConfiguration;

    private final HashSet<String> tableNames;

    public HBasePersistenceEngine(final List<String> zookeeperHosts, final int zookeeperClientPort) throws Exception {

        final String quorumHosts;

        if(zookeeperHosts.size() > 1) {
            quorumHosts = StringUtils.join(",", zookeeperHosts);
        } else {
            quorumHosts = zookeeperHosts.get(0);
        }

        this.hbaseConfiguration = HBaseConfiguration.create();
        this.hbaseConfiguration.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, quorumHosts);
        this.hbaseConfiguration.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, String.valueOf(zookeeperClientPort));

        this.hBaseConnection = HConnectionManager.createConnection(hbaseConfiguration);

        this.tableNames = getTables();
    }

    @Override
    public String getIdentifier() {
        String result = "HBase";
        return result;
    }

    @Override
    public boolean write(final Job job) {
        boolean success = true;

        final String tableName = tableForFunction(job.getFunctionName());

        try {
            createTable(tableName);
            HTableInterface table = hBaseConnection.getTable(tableName);
            // Key = unique id
            Put b = new Put(Bytes.toBytes(job.getUniqueID()));
            putColumn(b, "properties", "job_handle", job.getJobHandle());
            putColumn(b, "properties", "queue_name", job.getFunctionName());
            putColumn(b, "properties", "data", job.getData());
            putColumn(b, "properties", "priority", job.getPriority().toString());

            table.put(b);
            table.close();
        } catch (IOException e) {
            success = false;
        }

        return success;
    }

    private void putColumn(final Put p, final String family, final String column, final String value) {
        p.add(
                Bytes.toBytes(family),
                Bytes.toBytes(column),
                Bytes.toBytes(value)
        );
    }

    private void putColumn(final Put p, final String family, final String column, final byte[] value) {
        p.add(
                Bytes.toBytes(family),
                Bytes.toBytes(column),
                value
        );
    }




    @Override
    public void delete(final Job job) {
        final String tableName = tableForFunction(job.getFunctionName());
        try {
            HTableInterface table = hBaseConnection.getTable(tableName);
            Delete d = new Delete(Bytes.toBytes(job.getUniqueID()));
            table.delete(d);
            table.close();
        } catch (IOException e) {
            LOG.error("IO Exception deleting job " + job.getUniqueID() + ":", e);
        }
    }

    @Override
    public void delete(final String functionName, final String uniqueID) {

    }

    @Override
    public void deleteAll() {

    }

    @Override
    public Job findJob(final String functionName, final String uniqueID) {
        Job job = null;
        final String tableName = tableForFunction(functionName);
        try {
            HTableInterface table = hBaseConnection.getTable(tableName);

            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(uniqueID));
            // Don't pre-fetch more than 1 row.
            scan.setCaching(1);

            ResultScanner resultScanner = table.getScanner(scan);
            Result result = resultScanner.next();

            if(result != null) {
                NavigableMap<byte[], byte[]> jobmap =
                        result.getFamilyMap(Bytes.toBytes("properties"));

                byte[] data = null;
                String jobHandle = null;
                JobPriority jobPriority = null;

                // TODO: Optimize...
                for (byte[] key : jobmap.keySet()) {
                    String keyStr = Bytes.toString(key);
                    switch(keyStr) {
                        case "data":
                            data = jobmap.get(key);
                            break;
                        case "priority":
                            jobPriority = JobPriority.valueOf(Bytes.toString(jobmap.get(key)));
                            break;
                        case "job_handle":
                            jobHandle = Bytes.toString(jobmap.get(key));
                            break;
                    }

                }

                if (jobHandle != null && jobPriority != null && data != null) {
                    job = new Job.Builder()
                            .jobHandle(jobHandle)
                            .data(data)
                            .functionName(functionName)
                            .priority(jobPriority)
                            .uniqueID(uniqueID)
                            .build();
                }
            }

            resultScanner.close();
            table.close();
        } catch (IOException e) {
            LOG.error("Error loading job (Q: " + functionName + " ID: " + uniqueID + "): ", e);
        }

        return job;
    }

    @Override
    public Collection<QueuedJob> readAll() {
        LinkedList<QueuedJob> jobs = new LinkedList<>();

        return jobs;
    }

    @Override
    public Collection<QueuedJob> getAllForFunction(final String functionName) {
        LinkedList<QueuedJob> jobs = new LinkedList<>();

        return jobs;
    }

    private HashSet<String> getTables() {
        HashSet<String> tableNamesSet = new HashSet<>();

        try {
            HBaseAdmin admin = new HBaseAdmin(hbaseConfiguration);
            TableName[] tableNames = admin.listTableNames();

            for(TableName tn : tableNames) {
                String name = tn.getNameAsString();
                if (name.startsWith(TABLE_PREFIX)) {
                    tableNamesSet.add(name);
                }
            }
        } catch (IOException e) {
            LOG.error("I/O error: ", e);
        }

        return tableNamesSet;

    }

    private String tableForFunction(final String functionName) {
        return format("%s%s", TABLE_PREFIX, functionName);
    }

    private boolean tableExists(final String tableName) {
        if(tableNames.contains(tableName)) {
            return true;
        } else {
            try {
                HBaseAdmin admin = new HBaseAdmin(hbaseConfiguration);
                HTableDescriptor[] tables = admin.listTables(tableName);
                if (tables.length != 0) {
                    tableNames.add(tableName);
                    return true;
                } else {
                    return false;
                }
            } catch (IOException e) {
                return false;
            }
        }
    }

    private boolean createTable(final String tableName) {
        boolean success = true;

        try {
            if (!tableExists(tableName)) {
                LOG.info("Creating table " + tableName);
                HBaseAdmin admin = new HBaseAdmin(hbaseConfiguration);
                HTableDescriptor jobsTable = new HTableDescriptor(tableName);
                admin.createTable(jobsTable);

                // Cannot edit a structure on an active table, disable first.
                admin.disableTable(tableName);

                HColumnDescriptor propsDesc = new HColumnDescriptor("properties");
                admin.addColumn(tableName, propsDesc);

                // For reading, it needs to be re-enabled.
                admin.enableTable(tableName);
            }
        } catch (IOException e) {
            success = false;
        }

        return success;
    }

}
