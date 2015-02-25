package net.johnewart.gearman.engine.storage;

import com.google.common.collect.ImmutableList;
import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.LinkedList;

public class PostgresExceptionStorageEngine implements ExceptionStorageEngine {
    private static Logger LOG = LoggerFactory.getLogger(PostgresExceptionStorageEngine.class);
    private final BoneCP connectionPool;

    private final String tableName;
    private final String readPageOfExceptionsQuery;
    private final String insertQuery;
    private final String fetchJobHandlesQuery;
    private final String countQuery;
    private final String createTableQuery;
    private final String createUidIndexQuery;
    private final String createJobHandleIndexQuery;

    public PostgresExceptionStorageEngine(final String hostname,
                                     final int port,
                                     final String database,
                                     final String user,
                                     final String password,
                                     final String tableName) throws SQLException
    {
        final String url = "jdbc:postgresql://" + hostname + ":" + port + "/" + database;

        final BoneCPConfig config = new BoneCPConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setMinConnectionsPerPartition(10);
        config.setMaxConnectionsPerPartition(20);
        config.setPartitionCount(1);

        connectionPool = new BoneCP(config);

        this.tableName = tableName;
        this.readPageOfExceptionsQuery =
                String.format("SELECT * FROM %s ORDER BY exception_time LIMIT ? OFFSET ?", tableName);
        this.insertQuery =
                String.format("INSERT INTO %s(job_handle, unique_id, job_data, exception_data, exception_time) VALUES (?,?,?,?,?)",
                        tableName);
        this.fetchJobHandlesQuery =
                String.format("SELECT job_handle FROM %s", tableName);
        this.countQuery =
                String.format("SELECT COUNT(*) AS exceptionCount FROM %s", tableName);
        this.createTableQuery =
                String.format("CREATE TABLE %s(id bigserial, job_handle text, unique_id text, job_data bytea, exception_data bytea, exception_time bigint)", tableName);
        this.createUidIndexQuery =
                String.format("CREATE INDEX %s_unique_id ON %s(unique_id)", tableName, tableName);
        this.createJobHandleIndexQuery =
                String.format("CREATE INDEX %s_job_handle ON %s(job_handle)", tableName, tableName);


        if (!validateOrCreateTable()) {
            throw new SQLException("Unable to validate or create exceptions table. Check credentials.");
        }
    }

    @Override
    public boolean storeException(String jobHandle, String uniqueId, byte[] jobData, byte[] exceptionData) {
        PreparedStatement st = null;
        Connection conn = null;
        DateTime when = DateTime.now();

        try {
            conn = connectionPool.getConnection();
            if(conn != null)
            {
                // Update an existing job if one exists based on unique id
                st = conn.prepareStatement(insertQuery);
                st.setString(1, jobHandle);
                st.setString(2, uniqueId);
                st.setBytes(3, jobData);
                st.setBytes(4, exceptionData);
                st.setLong(5, when.getMillis());
                int inserted = st.executeUpdate();
                return inserted != 0;
            } else {
                return false;
            }
        } catch (SQLException se) {
            LOG.error("SQL Error writing exception: " , se);
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
    public ImmutableList<String> getFailedJobHandles() {
        LinkedList<String> jobHandles = new LinkedList<>();
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            conn = connectionPool.getConnection();
            if(conn != null)
            {
                LOG.debug("Reading all job data from PostgreSQL");

                st = conn.prepareStatement(fetchJobHandlesQuery);
                rs = st.executeQuery();

                while(rs.next())
                {
                    final String jobHandle = rs.getString("job_handle");
                    jobHandles.add(jobHandle);
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

        return ImmutableList.copyOf(jobHandles);
    }

    @Override
    public ImmutableList<ExceptionData> getExceptions(int pageNum, int pageSize) {
        LinkedList<ExceptionData> exceptionDataList = new LinkedList<>();
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            conn = connectionPool.getConnection();
            if(conn != null)
            {
                st = conn.prepareStatement(readPageOfExceptionsQuery);
                st.setInt(1, pageSize);
                st.setInt(2, ((pageNum - 1) * pageSize));

                rs = st.executeQuery();

                while(rs.next())
                {

                    try {
                        final String uniqueId = rs.getString("unique_id");
                        final String jobHandle = rs.getString("job_handle");
                        final LocalDateTime when = new LocalDateTime(rs.getLong("exception_time"));
                        final byte[] exceptionData = rs.getBytes("exception_data");
                        final byte[] jobData = rs.getBytes("job_data");

                        exceptionDataList.add(new ExceptionData(jobHandle, uniqueId, jobData, exceptionData, when));
                    } catch (Exception e) {
                        LOG.error("Unable to load job '" + rs.getString("unique_id") + "'");
                    }
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

        return ImmutableList.copyOf(exceptionDataList);
    }

    @Override
    public int getCount() {
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            conn = connectionPool.getConnection();
            if (conn != null) {

                st = conn.prepareStatement(countQuery);
                rs = st.executeQuery();

                if (rs.next()) {
                    return rs.getInt("exceptionCount");
                } else {
                    return -1;
                }
            } else {
                return -1;
            }

        } catch (SQLException se) {
            LOG.debug(se.toString());
            return -1;
        } finally {
            try {
                if (rs != null)
                    rs.close();

                if (st != null)
                    st.close();

                if (conn != null)
                    conn.close();

            } catch (SQLException innerEx) {
                LOG.debug("Error cleaning up: " + innerEx);
            }
        }
    }

    private boolean validateOrCreateTable()
    {
        PreparedStatement st = null;
        Connection conn = null;
        boolean success = false;

        try {
            conn = connectionPool.getConnection();
            if(conn != null)
            {
                DatabaseMetaData dbm = conn.getMetaData();
                ResultSet tables = dbm.getTables(null, null, tableName, null);
                if(!tables.next())
                {
                    st = conn.prepareStatement(createTableQuery);
                    st.executeUpdate();
                    st = conn.prepareStatement(createUidIndexQuery);
                    st.executeUpdate();
                    st = conn.prepareStatement(createJobHandleIndexQuery);
                    st.executeUpdate();

                    // Make sure it worked
                    ResultSet createdTables = dbm.getTables(null, null, tableName, null);
                    if(createdTables.next()) {
                        LOG.debug("Created exceptions table: " + tableName);
                        success = true;
                    } else {
                        LOG.debug("Unable to create exceptions table: " + tableName);
                        success = false;
                    }
                }  else {
                    LOG.debug("Exceptions table '" + tableName + "' already exists.");
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
