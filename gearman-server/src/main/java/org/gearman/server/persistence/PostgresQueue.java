package org.gearman.server.persistence;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.gearman.server.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Collection;
import java.util.LinkedList;

public class PostgresQueue implements PersistenceEngine {
    private static Logger LOG = LoggerFactory.getLogger(PostgresQueue.class);

    private final String url;
    private final String user;
    private final String password;

    public PostgresQueue(String hostname, int port, String database, String user, String password)
    {
        this.url = "jdbc:postgresql://" + hostname + ":" + port + "/" + database;
        this.user = user;
        this.password = password;
    }

    @Override
    public void write(Job job) {
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;
        ObjectMapper mapper = new ObjectMapper();

        try {
            conn = DriverManager.getConnection(url, user, password);
            st = conn.prepareStatement("INSERT INTO jobs (unique_id, function_name, job_handle, json_data) VALUES (?, ?, ?, ?)");
            st.setString(1, job.getUniqueID());
            st.setString(2, job.getFunctionName());
            st.setString(3, job.getJobHandle());
            st.setString(4, mapper.writeValueAsString(job));
            int inserted = st.executeUpdate();
            LOG.debug("Inserted " + inserted + " records for UUID " + job.getUniqueID());
        } catch (SQLException se) {
            se.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(st != null)
                    st.close();

                if(conn != null)
                    conn.close();

            } catch (SQLException innerEx) {

            }
        }
    }

    @Override
    public void delete(Job job) {
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            conn = DriverManager.getConnection(url, user, password);
            st = conn.prepareStatement("DELETE FROM jobs WHERE unique_id = ?");
            st.setString(1, job.getUniqueID());
            int deleted = st.executeUpdate();
            LOG.debug("Deleted " + deleted + " records for UUID " + job.getUniqueID());
        } catch (SQLException se) {

        } finally {
            try {
                if(st != null)
                    st.close();

                if(conn != null)
                    conn.close();

            } catch (SQLException innerEx) {

            }
        }
    }

    @Override
    public void deleteAll() {
        Statement st = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            st = conn.createStatement();
            int deleted = st.executeUpdate("DELETE FROM jobs");
            LOG.debug("Deleted " + deleted + " jobs...");
        } catch (SQLException se) {

        } finally {
            try {
                if(rs != null)
                    rs.close();

                if(st != null)
                    st.close();

                if(conn != null)
                    conn.close();

            } catch (SQLException innerEx) {
                LOG.debug("Error cleaning up: " + innerEx);
            }
        }
    }

    @Override
    public Job findJob(String functionName, String uniqueID) {
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;

        Job job = null;

        try {
            conn = DriverManager.getConnection(url, user, password);
            st = conn.prepareStatement("SELECT * FROM jobs WHERE function_name = ? AND unique_id = ?");
            st.setString(1, functionName);
            st.setString(2, uniqueID);

            ObjectMapper mapper = new ObjectMapper();
            rs = st.executeQuery();

            if(rs.next())
            {
                String jobJSON = rs.getString("json_data");
                job = mapper.readValue(jobJSON, Job.class);
            } else {
                LOG.warn("No job for unique ID: " + uniqueID + " -- this could be an internal consistency problem...");
            }
        } catch (SQLException se) {
            LOG.debug(se.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(rs != null)
                    rs.close();

                if(st != null)
                    st.close();

                if(conn != null)
                    conn.close();

            } catch (SQLException innerEx) {
                LOG.debug("Error cleaning up: " + innerEx);
            }
        }

        return job;
    }

    @Override
    public Collection<Job> readAll() {
        LinkedList<Job> jobs = new LinkedList<>();
        Statement st = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            conn = DriverManager.getConnection(url, user, password);
            st = conn.createStatement();
            rs = st.executeQuery("SELECT * FROM jobs");
            ObjectMapper mapper = new ObjectMapper();

            while(rs.next())
            {
                String jobJSON = rs.getString("json_data");
                Job job = mapper.readValue(jobJSON, Job.class);
                jobs.add(job);
            }

        } catch (SQLException se) {
            LOG.debug(se.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(rs != null)
                    rs.close();

                if(st != null)
                    st.close();

                if(conn != null)
                    conn.close();

            } catch (SQLException innerEx) {
                LOG.debug("Error cleaning up: " + innerEx);
            }
        }

        return jobs;
    }

    @Override
    public Collection<Job> getAllForFunction(String functionName) {
        LinkedList<Job> jobs = new LinkedList<>();
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            conn = DriverManager.getConnection(url, user, password);
            st = conn.prepareStatement("SELECT * FROM jobs WHERE function_name = ?");
            st.setString(1, functionName);
            ObjectMapper mapper = new ObjectMapper();

            while(rs.next())
            {
                String jobJSON = rs.getString("json_data");
                Job job = mapper.readValue(jobJSON, Job.class);
                jobs.add(job);
            }

        } catch (SQLException se) {
            LOG.debug(se.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(rs != null)
                    rs.close();

                if(st != null)
                    st.close();

                if(conn != null)
                    conn.close();

            } catch (SQLException innerEx) {
                LOG.debug("Error cleaning up: " + innerEx);
            }
        }

        return jobs;
    }
}
