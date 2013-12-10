package net.johnewart.gearman.server.config.persistence;

public class PostgreSQLConfiguration {
    public String host;
    public int port;
    private String dbName;
    private String password;
    private String user;


    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbname) {
        this.dbName = dbname;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
