package org.gearman.server.persistence;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.gearman.constants.JobPriority;
import org.gearman.common.Job;
import org.gearman.server.core.QueuedJob;
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
    private BoneCP connectionPool;
    private final int jobsPerPage =5000;

    public PostgresQueue(String hostname, int port, String database, String user, String password)
    {

        this.url = "jdbc:postgresql://" + hostname + ":" + port + "/" + database;
        this.user = user;
        this.password = password;
        try {
            BoneCPConfig config = new BoneCPConfig();
            config.setJdbcUrl(this.url);
            config.setUsername(this.user);
            config.setPassword(this.password);
            config.setMinConnectionsPerPartition(10);
            config.setMaxConnectionsPerPartition(20);
            config.setPartitionCount(1);
            connectionPool = new BoneCP(config);
            if(!validateOrCreateTable())
            {
                throw new SQLException("Unable to validate or create jobs table. Check credentials.");
            }
        } catch (SQLException se) {
            se.printStackTrace();
            connectionPool = null;
        }
    }

    @Override
    public String getIdentifier() {
        String result = url;

        try {
            Connection connection = connectionPool.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            int majorVersion, minorVersion;
            String productName, productVersion;

            majorVersion = metaData.getDatabaseMajorVersion();
            minorVersion = metaData.getDatabaseMinorVersion();
            productName = metaData.getDatabaseProductName();
            productVersion = metaData.getDatabaseProductVersion();
            result = String.format("%s (%s v%d.%d) - %s", productName, productVersion, majorVersion, minorVersion, url);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    @Override
    public void write(Job job) {
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;
        ObjectMapper mapper = new ObjectMapper();

        try {
            conn = connectionPool.getConnection();
            if(conn != null)
            {
                String jobJSON = mapper.writeValueAsString(job);

                // Update an existing job if one exists based on unique id
                st = conn.prepareStatement("UPDATE jobs SET job_handle = ?, priority = ?, time_to_run = ?, json_data = ? WHERE unique_id = ? AND function_name = ?");
                st.setString(1, job.getJobHandle());
                st.setString(2, job.getPriority().toString());
                st.setLong  (3, job.getTimeToRun());
                st.setString(4, jobJSON);
                st.setString(5, job.getUniqueID());
                st.setString(6, job.getFunctionName());
                int updated = st.executeUpdate();

                // No updates, insert a new record.
                if(updated == 0)
                {
                    st = conn.prepareStatement("INSERT INTO jobs (unique_id, function_name, time_to_run, priority, job_handle, json_data) VALUES (?, ?, ?, ?, ?, ?)");
                    st.setString(1, job.getUniqueID());
                    st.setString(2, job.getFunctionName());
                    st.setLong(3, job.getTimeToRun());
                    st.setString(4, job.getPriority().toString());
                    st.setString(5, job.getJobHandle());
                    st.setString(6, jobJSON);
                    int inserted = st.executeUpdate();
                    LOG.debug("Inserted " + inserted + " records for UUID " + job.getUniqueID());
                }
            }
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

    }

    @Override
    public void delete(String functionName, String uniqueID) {
        PreparedStatement st = null;
        Connection conn = null;

        try {
            conn = connectionPool.getConnection();
            if(conn != null)
            {
                st = conn.prepareStatement("DELETE FROM jobs WHERE function_name = ? AND unique_id = ?");
                st.setString(1, functionName);
                st.setString(2, uniqueID);
                int deleted = st.executeUpdate();
                LOG.debug("Deleted " + deleted + " records for " + functionName + "/" +uniqueID);
            }
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
            conn = connectionPool.getConnection();
            if(conn != null)
            {
                st = conn.createStatement();
                int deleted = st.executeUpdate("DELETE FROM jobs");
                LOG.debug("Deleted " + deleted + " jobs...");
            }
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
            conn = connectionPool.getConnection();
            if(conn != null)
            {
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
    public Collection<QueuedJob> readAll() {
        LinkedList<QueuedJob> jobs = new LinkedList<>();
        Statement st = null;
        ResultSet rs = null;
        Connection conn = null;
        // Which page of results are we on?
        int pageNum = 0;

        try {
            conn = connectionPool.getConnection();
            if(conn != null)
            {
                st = conn.createStatement();
                st.setFetchSize(jobsPerPage);
                st.setMaxRows(jobsPerPage);

                LOG.debug("Reading all job data from PostgreSQL");

                rs = st.executeQuery("SELECT COUNT(*) AS jobCount FROM jobs");
                if(rs.next())
                {
                    int totalJobs = rs.getInt("jobCount");
                    int fetchedJobs = 0;
                    LOG.debug("Reading " + totalJobs + " jobs from PostgreSQL");
                    QueuedJob currentJob;
                    do {
                        rs = st.executeQuery("SELECT function_name, priority, unique_id, time_to_run FROM jobs LIMIT " + jobsPerPage + " OFFSET " + (pageNum * jobsPerPage));

                        while(rs.next())
                        {

                            try {
                                currentJob = new QueuedJob(rs.getString("unique_id"), rs.getLong("time_to_run"), JobPriority.valueOf(rs.getString("priority")), rs.getString("function_name"));
                                jobs.add(currentJob);
                            } catch (Exception e) {
                                LOG.error("Unable to load job '" + rs.getString("unique_id") + "'");
                            }
                            fetchedJobs += 1;
                        }

                        pageNum += 1;
                        LOG.debug("Loaded " + fetchedJobs + "...");
                    } while(fetchedJobs != totalJobs);
                }

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

    @Override
    public Collection<QueuedJob> getAllForFunction(String functionName) {
        LinkedList<QueuedJob> jobs = new LinkedList<>();
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;
        QueuedJob job;

        try {
            conn = connectionPool.getConnection();
            if(conn != null)
            {
                st = conn.prepareStatement("SELECT unique_id, time_to_run, priority FROM jobs WHERE function_name = ?");
                st.setString(1, functionName);
                ObjectMapper mapper = new ObjectMapper();
                rs = st.executeQuery();

                while(rs.next())
                {
                    job = new QueuedJob(rs.getString("unique_id"), rs.getLong("time_to_run"), JobPriority.valueOf(rs.getString("priority")), functionName);
                    jobs.add(job);
                }
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

    @Override
    public Job findJobByHandle(String jobHandle) {
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;

        Job job = null;

        try {
            conn = connectionPool.getConnection();
            if(conn != null)
            {
                st = conn.prepareStatement("SELECT * FROM jobs WHERE job_handle = ?");
                st.setString(1, jobHandle);

                ObjectMapper mapper = new ObjectMapper();
                rs = st.executeQuery();

                if(rs.next())
                {
                    String jobJSON = rs.getString("json_data");
                    job = mapper.readValue(jobJSON, Job.class);
                } else {
                    LOG.warn("No job for job handle: " + jobHandle + " -- this could be an internal consistency problem...");
                }
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

        return job;    }

    private boolean validateOrCreateTable()
    {
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;
        ObjectMapper mapper = new ObjectMapper();
        boolean success = false;

        try {
            conn = connectionPool.getConnection();
            if(conn != null)
            {
                DatabaseMetaData dbm = conn.getMetaData();
                ResultSet tables = dbm.getTables(null, null, "jobs", null);
                if(!tables.next())
                {
                    st = conn.prepareStatement("CREATE TABLE jobs(id bigserial, unique_id varchar(255), priority varchar(50), function_name varchar(255), time_to_run bigint, job_handle text, json_data text)");
                    int created = st.executeUpdate();
                    st = conn.prepareStatement("CREATE INDEX jobs_unique_id ON jobs(unique_id)");
                    int createdUniqueIDIndex = st.executeUpdate();
                    st = conn.prepareStatement("CREATE INDEX jobs_job_handle ON jobs(job_handle)");
                    int createdJobHandleIndex = st.executeUpdate();

                    if(created > 0)
                    {
                        LOG.debug("Created jobs table");
                        success = true;
                    } else {
                        LOG.debug("Unable to create jobs table.");
                        success = false;
                    }
                }  else {
                    LOG.debug("Jobs table already exists.");
                    success = true;
                }
            }
        } catch (SQLException se) {
            se.printStackTrace();
        } finally {
            try {
                if(st != null)
                    st.close();

                if(conn != null)
                    conn.close();

            } catch (SQLException innerEx) {
                innerEx.printStackTrace();
            }
        }

        return success;
    }
}
