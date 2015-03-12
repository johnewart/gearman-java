package net.johnewart.gearman.server.config.persistence;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.regions.ServiceAbbreviations;

public class DynamoDBConfiguration {
    private String endpoint;
    private String secretKey;
    private String accessKey;
    private String table = "jobs";
    private String region;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getEndpoint() {
        // If no endpoint manually specified, then try to discover
        // based on region (if specified)
        if (this.endpoint == null && region != null) {
            this.endpoint =
                    Region.getRegion(Regions.fromName(region))
                            .getServiceEndpoint(ServiceAbbreviations.Dynamodb);
        }

        return endpoint;

    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getRegion() { return region; }

    public void setRegion(String region) {
        this.region = region;
    }
}
