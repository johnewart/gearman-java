package org.gearman.server.persistence;

import org.gearman.server.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryQueue implements PersistenceEngine {
    private final ConcurrentHashMap<String, HashMap<String, Job>> jobHash;
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
        HashMap<String, Job> funcHash = getFunctionHash(job.getFunctionName());
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
        HashMap<String, Job> funcHash = getFunctionHash(functionName);


        if(funcHash != null && funcHash.containsKey(uniqueID))
        {
            job = funcHash.get(uniqueID);
        }

        return job;
    }

    @Override
    public Collection<Job> readAll() {
        return new ArrayList<Job>();
    }

    @Override
    public Collection<Job> getAllForFunction(String functionName) {
        HashMap<String, Job> funcHash = getFunctionHash(functionName);

        if(funcHash != null)
        {
            return funcHash.values();
        } else {
            return null;
        }
    }

    @Override
    public Job findJobByHandle(String jobHandle) {
        return jobHandleMap.get(jobHandle);
    }

    private HashMap<String, Job> getFunctionHash(String functionName)
    {
        HashMap<String, Job> hash = null;
        if(jobHash.containsKey(functionName))
        {
            hash = jobHash.get(functionName);
        } else {
            hash = new HashMap<>();
            jobHash.put(functionName, hash);
        }

        return hash;
    }
}
