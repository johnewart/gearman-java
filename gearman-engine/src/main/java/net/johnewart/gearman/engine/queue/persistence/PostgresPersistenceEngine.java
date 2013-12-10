package net.johnewart.gearman.engine.queue.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.engine.core.QueuedJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Collection;
import java.util.LinkedList;

public class PostgresPersistenceEngine implements PersistenceEngine {
    private static Logger LOG = LoggerFactory.getLogger(PostgresPersistenceEngine.class);
    private static final int JOBS_PER_PAGE = 5000;
    private final String url;
    private final BoneCP connectionPool;

    public PostgresPersistenceEngine(final String hostname,
                                     final int port,
                                     final String database,
                                     final String user,
                                     final String password) throws SQLException
    {

        this.url = "jdbc:postgresql://" + hostname + ":" + port + "/" + database;

        final BoneCPConfig config = new BoneCPConfig();
        config.setJdbcUrl(this.url);
        config.setUsername(user);
        config.setPassword(password);
        config.setMinConnectionsPerPartition(10);
        config.setMaxConnectionsPerPartition(20);
        config.setPartitionCount(1);

        connectionPool = new BoneCP(config);

        if (!validateOrCreateTable()) {
            throw new SQLException("Unable to validate or create jobs table. Check credentials.");
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
    public boolean write(final Job job) {
        PreparedStatement st = null;
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

            return true;
        } catch (SQLException se) {
            LOG.error("SQL Error writing job: " , se);
            return false;
        } catch (IOException e) {
            LOG.error("I/O Error writing job: " , e);
            return false;
        } finally {
            try {
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
    public void delete(final Job job) {

    }

    @Override
    public void delete(final String functionName, final String uniqueID) {
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
            LOG.error("SQL Error deleting job: " , se);
        } finally {
            try {
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
    public void deleteAll() {
        Statement st = null;
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
            LOG.error("SQL Error deleting all jobs: " , se);
        } finally {
            try {
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
    public Job findJob(final String functionName, final String uniqueID) {
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
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;
        // Which page of results are we on?
        int pageNum = 0;

        try {
            conn = connectionPool.getConnection();
            if(conn != null)
            {

                LOG.debug("Reading all job data from PostgreSQL");
                final String countQuery = "SELECT COUNT(*) AS jobCount FROM jobs";
                st = conn.prepareStatement(countQuery);
                rs = st.executeQuery();

                if(rs.next())
                {
                    int totalJobs = rs.getInt("jobCount");
                    int fetchedJobs = 0;
                    LOG.debug("Reading " + totalJobs + " jobs from PostgreSQL");
                    do {
                        final String readQuery =
                                "SELECT function_name, priority, unique_id, time_to_run " +
                                "  FROM jobs " +
                                " LIMIT ? " +
                                "OFFSET ?";

                        st.setFetchSize(JOBS_PER_PAGE);
                        st.setMaxRows(JOBS_PER_PAGE);

                        st = conn.prepareStatement(readQuery);
                        st.setInt(1, JOBS_PER_PAGE);
                        st.setInt(2, (pageNum * JOBS_PER_PAGE));

                        rs = st.executeQuery();

                        while(rs.next())
                        {

                            try {
                                final String uniqueId = rs.getString("unique_id");
                                final long timeToRun = rs.getLong("time_to_run");
                                final JobPriority jobPriority = JobPriority.valueOf(rs.getString("priority"));
                                final String functionName = rs.getString("function_name");

                                jobs.add(new QueuedJob(uniqueId, timeToRun, jobPriority, functionName));
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
    public Collection<QueuedJob> getAllForFunction(final String functionName) {
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
                    LOG.warn("No job for job handle: " + jobHandle +
                             " -- this could be an internal consistency problem...");
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
