package net.johnewart.gearman.engine.queue.persistence;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.annotation.Timed;
import com.yammer.metrics.core.*;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.core.QueuedJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class DynamoDBPersistenceEngine implements PersistenceEngine {
    private static Logger LOG = LoggerFactory.getLogger(DynamoDBPersistenceEngine.class);
    private final String tableName;

    private final DynamoDB dynamoDB;
    private final AmazonDynamoDBClient client;
    private final DynamoDBMapper mapper;
    private final com.yammer.metrics.core.Timer writeTimer;

    public DynamoDBPersistenceEngine(final String endpoint,
                                     final String accessKey,
                                     final String secretKey,
                                     final String tableName,
                                     final Integer readUnits,
                                     final Integer writeUnits) throws SQLException
    {

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration clientConfig = new ClientConfiguration();
        client = new AmazonDynamoDBClient(credentials);
        //client.setRegion(Region.getRegion(Regions.US_WEST_2));
        client.setEndpoint(endpoint);
        this.dynamoDB = new DynamoDB(client);
        this.tableName = tableName;
        mapper = new DynamoDBMapper(client);
        writeTimer = Metrics.newTimer(DynamoDBPersistenceEngine.class, "dynamodb", "writes");

        if (!validateOrCreateTable(readUnits, writeUnits)) {
            throw new SQLException("Unable to validate or create jobs table '" + tableName + "'. Check credentials.");
        }
    }

    // TODO: Validation of things other than R/W units
    private boolean validateOrCreateTable(Integer readUnits, Integer writeUnits) {
        try {
            Table table =  dynamoDB.getTable(tableName);
            TableDescription tableDescription = table.describe();

            // Table exists?
            if (tableDescription != null) {
                // Ensure our r/w units are up to date

                if(tableDescription.getProvisionedThroughput().getWriteCapacityUnits() != writeUnits.longValue() ||
                   tableDescription.getProvisionedThroughput().getReadCapacityUnits() != readUnits.longValue()) {
                    ProvisionedThroughput throughput = new ProvisionedThroughput()
                            .withWriteCapacityUnits(writeUnits.longValue())
                            .withReadCapacityUnits(readUnits.longValue());
                    table.updateTable(throughput);

                    try {
                        table.waitForActive();
                        LOG.info(
                                String.format("Table %s already existed, updated R/W units of %d/%d units/sec",
                                        table.getTableName(),
                                        table.getDescription().getProvisionedThroughput().getReadCapacityUnits(),
                                        table.getDescription().getProvisionedThroughput().getWriteCapacityUnits()
                                )
                        );
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        return false;
                    }
                } else {
                    LOG.info(String.format("Table %s already exists and is up to date!", table.getTableName()));
                }

                return true;
            }
        } catch (ResourceNotFoundException nfe) {
            try {

                // If not, create it!
                ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
                attributeDefinitions.add(new AttributeDefinition()
                        .withAttributeName("JobKey")
                        .withAttributeType("S"));

                ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
                keySchema.add(new KeySchemaElement()
                        .withAttributeName("JobKey")
                        .withKeyType(KeyType.HASH));

                CreateTableRequest request = new CreateTableRequest()
                        .withTableName(tableName)
                        .withKeySchema(keySchema)
                        .withAttributeDefinitions(attributeDefinitions)
                        .withProvisionedThroughput(new ProvisionedThroughput()
                                .withReadCapacityUnits(readUnits.longValue())
                                .withWriteCapacityUnits(writeUnits.longValue()));

                System.out.println("Issuing CreateTable request for " + tableName);
                Table table = dynamoDB.createTable(request);

                System.out.println("Waiting for " + tableName
                        + " to be created...this may take a while...");
                table.waitForActive();


                TableDescription tableDescription = table.describe();
                LOG.info(
                    String.format("Created table '%s'(%s) with throughput %d read, %d write units/sec",
                            tableDescription.getTableName(),
                            tableDescription.getTableStatus(),
                            tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
                            tableDescription.getProvisionedThroughput().getWriteCapacityUnits()
                    )
                );

            } catch (Exception e) {
                System.err.println("CreateTable request failed for " + tableName);
                System.err.println(e.getMessage());
                return false;
            }
        }

        return true;
    }




    @Override
    public String getIdentifier() {
        return null;
    }

    @Timed(name = "dynamodb.write")
    @Override
    public boolean write(Job job) {
        long startTime = new Date().getTime();
        ObjectMapper objectMapper = new ObjectMapper();
        Table table = dynamoDB.getTable(tableName);

        try {
            String jobJSON = objectMapper.writeValueAsString(job);


            Item item = new Item()
                    .withPrimaryKey("JobKey", jobIdKey(job))
                    .withString("UniqueId", job.getUniqueID())
                    .withString("JobHandle", job.getJobHandle())
                    .withNumber("When", job.getTimeToRun())
                    .withString("Priority", job.getPriority().toString())
                    .withString("JobQueue", job.getFunctionName())
                    .withString("JSON", jobJSON);

            table.putItem(item);
            long timeDiff = new Date().getTime() - startTime;
            writeTimer.update(timeDiff, TimeUnit.MILLISECONDS);
            return true;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return false;
    }

    private String jobIdKey(Job job) {
        return jobIdKey(job.getFunctionName(), job.getUniqueID());
    }

    private String jobIdKey(String functionName, String uniqueId) {
        return new StringBuilder()
                        .append(functionName)
                        .append(uniqueId)
                        .toString();
    }

    @Override
    public void delete(Job job) {
        delete(job.getFunctionName(), job.getUniqueID());
    }

    @Override
    public void delete(String functionName, String uniqueID) {
        Table table = dynamoDB.getTable(tableName);
        table.deleteItem("JobKey", jobIdKey(functionName, uniqueID));
    }

    @Override
    public void deleteAll() {

    }

    @Override
    public Job findJob(String functionName, String uniqueID) {
        Table table = dynamoDB.getTable(tableName);
        Item item = table.getItem("JobKey", jobIdKey(functionName, uniqueID));
        return jobFromItem(item);

    }

    @Override
    public Collection<QueuedJob> readAll() {
        List<QueuedJob> queuedJobs = new LinkedList<>();
        Map<String, AttributeValue> lastKey = null;

        do {
            LOG.debug("Fetching a page of jobs from DynamoDB!");
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(tableName)
                    .withExclusiveStartKey(lastKey);

            ScanResult result = client.scan(scanRequest);

            for (Map<String, AttributeValue> item : result.getItems()) {
                queuedJobs.add(queuedJobFromMap(item));
            }

            lastKey = result.getLastEvaluatedKey();

        } while(lastKey != null);

        return queuedJobs;
    }

    private Job jobFromItem(Item item) {
        ObjectMapper mapper = new ObjectMapper();
        String json = item.getString("JSON");
        Job job = null;
        try {
            job = mapper.readValue(json, Job.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return job;
    }

    private QueuedJob queuedJobFromMap(Map<String, AttributeValue> item) {
        String uniqueId = item.get("UniqueId").getS();
        JobPriority priority = JobPriority.valueOf(item.get("Priority").getS());
        String jobQueue = item.get("JobQueue").getS();
        Long when = Long.valueOf(item.get("When").getN());

        return new QueuedJob(uniqueId, when, priority, jobQueue);
    }

    @Override
    public Collection<QueuedJob> getAllForFunction(String functionName) {
        return new LinkedList<>();
    }
}
