package net.johnewart.gearman.engine.queue.persistence;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.core.QueuedJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

public class DynamoDBPersistenceEngine implements PersistenceEngine {
    private static Logger LOG = LoggerFactory.getLogger(DynamoDBPersistenceEngine.class);
    private static final int JOBS_PER_PAGE = 5000;
    private final String tableName;

    private final DynamoDB dynamoDB;
    private final AmazonDynamoDBClient client;
    private final DynamoDBMapper mapper;

    public DynamoDBPersistenceEngine(final String endpoint,
                                     final String user,
                                     final String password,
                                     final String tableName) throws SQLException
    {

        AWSCredentials credentials = new BasicAWSCredentials(user, password);
        ClientConfiguration clientConfig = new ClientConfiguration();
        client = new AmazonDynamoDBClient(credentials);
        //client.setRegion(Region.getRegion(Regions.US_WEST_2));
        client.setEndpoint(endpoint);
        this.dynamoDB = new DynamoDB(client);
        this.tableName = tableName;
        mapper = new DynamoDBMapper(client);


        if (!validateOrCreateTable()) {
            throw new SQLException("Unable to validate or create jobs table '" + tableName + "'. Check credentials.");
        }
    }

    private boolean validateOrCreateTable() {
        try {

            ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
            attributeDefinitions.add(new AttributeDefinition()
                    .withAttributeName("UniqueId")
                    .withAttributeType("S"));

            ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
            keySchema.add(new KeySchemaElement()
                    .withAttributeName("UniqueId")
                    .withKeyType(KeyType.HASH));

            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(keySchema)
                    .withAttributeDefinitions(attributeDefinitions)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(5L)
                            .withWriteCapacityUnits(6L));

            System.out.println("Issuing CreateTable request for " + tableName);
            Table table = dynamoDB.createTable(request);

            System.out.println("Waiting for " + tableName
                    + " to be created...this may take a while...");
            table.waitForActive();

            getTableInformation();

        } catch (Exception e) {
            System.err.println("CreateTable request failed for " + tableName);
            System.err.println(e.getMessage());
            return false;
        }

        return true;

    }

    private void listMyTables() {

        TableCollection<ListTablesResult> tables = dynamoDB.listTables();
        Iterator<Table> iterator = tables.iterator();

        System.out.println("Listing table names");

        while (iterator.hasNext()) {
            Table table = iterator.next();
            System.out.println(table.getTableName());
        }
    }

    private void getTableInformation() {

        System.out.println("Describing " + tableName);

        TableDescription tableDescription = dynamoDB.getTable(tableName).describe();
        System.out.format("Name: %s:\n" + "Status: %s \n"
                        + "Provisioned Throughput (read capacity units/sec): %d \n"
                        + "Provisioned Throughput (write capacity units/sec): %d \n",
                tableDescription.getTableName(),
                tableDescription.getTableStatus(),
                tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
                tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
    }



    @Override
    public String getIdentifier() {
        return null;
    }

    @Override
    public boolean write(Job job) {
        ObjectMapper objectMapper = new ObjectMapper();
        Table table = dynamoDB.getTable(tableName);

        try {
            String jobJSON = objectMapper.writeValueAsString(job);

            Item item = new Item()
                    .withPrimaryKey("UniqueId", job.getUniqueID())
                    .withString("JobHandle", job.getJobHandle())
                    .withNumber("When", job.getTimeToRun())
                    .withString("Priority", job.getPriority().toString())
                    .withString("JobQueue", job.getFunctionName())
                    .withString("JSON", jobJSON);

            table.putItem(item);
            return true;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public void delete(Job job) {

    }

    @Override
    public void delete(String functionName, String uniqueID) {

    }

    @Override
    public void deleteAll() {

    }

    @Override
    public Job findJob(String functionName, String uniqueID) {
        return null;
    }

    @Override
    public Collection<QueuedJob> readAll() {
        return new LinkedList<>();
    }

    @Override
    public Collection<QueuedJob> getAllForFunction(String functionName) {
        return new LinkedList<>();
    }
}
