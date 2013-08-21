package net.johnewart.gearman.server.persistence;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.server.core.QueuedJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
    public String getIdentifier() {
        return "Memory-only";
    }

    @Override
    public void write(Job job) {
        getFunctionHash(job.getFunctionName()).put(job.getUniqueID(), job);
        jobHandleMap.put(job.getJobHandle(), job);
    }

    @Override
    public void delete(Job job) {
        delete(job.getFunctionName(), job.getUniqueID());
    }

    @Override
    public void delete(String functionName, String uniqueID) {
        Map<String, Job> funcHash = getFunctionHash(functionName);
        if(funcHash.containsKey(uniqueID))
        {
            Job job = funcHash.get(uniqueID);

            if(jobHandleMap.containsKey(job.getJobHandle()))
            {
                jobHandleMap.remove(job.getJobHandle());
            }

            funcHash.remove(uniqueID);
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
    public Collection<QueuedJob> readAll() {
        Set<QueuedJob> allJobs = new HashSet<>();

        for(String functionName : jobHash.keySet())
        {
            allJobs.addAll(getAllForFunction(functionName));
        }

        return allJobs;
    }

    @Override
    public Collection<QueuedJob> getAllForFunction(String functionName) {
        ConcurrentHashMap<String, Job> funcHash = getFunctionHash(functionName);
        ArrayList<QueuedJob> runnableJobs = new ArrayList<>();

        for( Job job : funcHash.values())
        {
            runnableJobs.add(new QueuedJob(job));
        }

        return runnableJobs;
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
