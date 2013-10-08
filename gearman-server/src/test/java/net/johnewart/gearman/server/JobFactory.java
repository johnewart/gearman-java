package net.johnewart.gearman.server;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.util.JobHandleFactory;
import org.joda.time.DateTime;
import org.joda.time.Seconds;

import java.util.UUID;

public class JobFactory {
    public static Job generateForegroundJob(String functionName)
    {
        String uuid = UUID.randomUUID().toString();
        byte[] data = {'f','o','o'};
        JobPriority priority = JobPriority.NORMAL;
        boolean isBackground = false;
        return new Job(functionName, uuid, data, JobHandleFactory.getNextJobHandle(),  priority, isBackground, -1);
    }

    public static Job generateBackgroundJob(String functionName)
    {
        String uuid = UUID.randomUUID().toString();
        byte[] data = {'b','a','r'};
        JobPriority priority = JobPriority.NORMAL;
        boolean isBackground = true;
        return new Job(functionName, uuid, data, JobHandleFactory.getNextJobHandle(), priority, isBackground, -1);
    }

    public static Job generateFutureJob(String functionName, Seconds seconds) {
        String uuid = UUID.randomUUID().toString();
        byte[] data = {'f','l','u','x',' ','c','a','p'};
        JobPriority priority = JobPriority.NORMAL;
        boolean isBackground = true;
        long whenToRun = new DateTime().plus(seconds).toDate().getTime() / 1000;
        return new Job(functionName, uuid, data, JobHandleFactory.getNextJobHandle(), priority, isBackground, whenToRun);
    }

    public static Job generateHighPriorityBackgroundJob(String functionName)
    {
        String uuid = UUID.randomUUID().toString();
        byte[] data = {'s','u','p','e','r'};
        JobPriority priority = JobPriority.HIGH;
        boolean isBackground = true;
        return new Job(functionName, uuid, data, JobHandleFactory.getNextJobHandle(), priority, isBackground, -1);    }

    public static Job generateLowPriorityBackgroundJob(String functionName)
    {
        String uuid = UUID.randomUUID().toString();
        byte[] data = {'s','u','p','e','r'};
        JobPriority priority = JobPriority.LOW;
        boolean isBackground = true;
        return new Job(functionName, uuid, data, JobHandleFactory.getNextJobHandle(), priority, isBackground, -1);
    }
}

