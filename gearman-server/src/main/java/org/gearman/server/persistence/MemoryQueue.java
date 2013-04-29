package org.gearman.server.persistence;

import org.gearman.server.Job;
import org.gearman.server.core.RunnableJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryQueue implements PersistenceEngine {
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Job>> jobHash;
    private final Logger LOG = LoggerFactory.getLogger(MemoryQueue.class);
    private final ConcurrentHashMap<String, Job> jobHandleMap;

    public MemoryQueue()
    {
        jobHash  = new ConcurrentHashMap<>();
        jobHandleMap = new ConcurrentHashMap<>();
    }

    @Override
    public void write(Job job) {
        getFunctionHash(job.getFunctionName()).put(job.getUniqueID(), job);
    }

    @Override
    public void delete(Job job) {
        ConcurrentHashMap<String, Job> funcHash = getFunctionHash(job.getFunctionName());
        if(funcHash.containsKey(job.getUniqueID()))
        {
            funcHash.remove(job.getUniqueID());
        }

        if(jobHandleMap.containsKey(job.getJobHandle()))
        {
            jobHandleMap.remove(job.getJobHandle());
        }
    }

    @Override
    public void deleteAll() {
        jobHash.clear();
        jobHandleMap.clear();
    }

    @Override
    public Job findJob(String functionName, String uniqueID) {
        Job job = null;
        ConcurrentHashMap<String, Job> funcHash = getFunctionHash(functionName);


        if(funcHash != null && funcHash.containsKey(uniqueID))
        {
            job = funcHash.get(uniqueID);
        }

        return job;
    }

    @Override
    public Collection<RunnableJob> readAll() {
        return new ArrayList<RunnableJob>();
    }

    @Override
    public Collection<RunnableJob> getAllForFunction(String functionName) {
        ConcurrentHashMap<String, Job> funcHash = getFunctionHash(functionName);
        ArrayList<RunnableJob> runnableJobs = new ArrayList<>();

        if(funcHash != null)
        {
            for( Job job : funcHash.values())
            {
                runnableJobs.add(job.getRunnableJob());
            }

            return runnableJobs;
        } else {
            return null;
        }

    }

    @Override
    public Job findJobByHandle(String jobHandle) {
        return jobHandleMap.get(jobHandle);
    }

    private ConcurrentHashMap<String, Job> getFunctionHash(String functionName)
    {
        ConcurrentHashMap<String, Job> hash = null;
        if(jobHash.containsKey(functionName))
        {
            hash = jobHash.get(functionName);
        } else {
            hash = new ConcurrentHashMap<>();
            jobHash.put(functionName, hash);
        }

        return hash;
    }
}
