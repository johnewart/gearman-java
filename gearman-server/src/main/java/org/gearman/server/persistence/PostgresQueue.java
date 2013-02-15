package org.gearman.server.persistence;

import org.codehaus.jackson.map.ObjectMapper;
import org.gearman.server.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 2/11/13
 * Time: 10:03 AM
 * To change this template use File | Settings | File Templates.
 */
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
    public void write(Job job) throws Exception {
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
    public void delete(Job job) throws Exception {
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
    public void deleteAll() throws Exception {
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

            }
        }
    }

    @Override
    public Collection<Job> readAll() throws Exception {
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
