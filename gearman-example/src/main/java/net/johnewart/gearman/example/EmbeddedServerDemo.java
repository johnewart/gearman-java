package net.johnewart.gearman.example;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.events.WorkEvent;
import net.johnewart.gearman.common.interfaces.GearmanClient;
import net.johnewart.gearman.common.interfaces.GearmanFunction;
import net.johnewart.gearman.embedded.EmbeddedGearmanClient;
import net.johnewart.gearman.embedded.EmbeddedGearmanServer;
import net.johnewart.gearman.embedded.EmbeddedGearmanWorker;
import net.johnewart.gearman.exceptions.JobSubmissionException;
import net.johnewart.gearman.exceptions.WorkException;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

public class EmbeddedServerDemo {
    EmbeddedGearmanServer server = new EmbeddedGearmanServer();

    public EmbeddedServerDemo() {
        GearmanClient client = new EmbeddedGearmanClient(server);
        EmbeddedGearmanWorker worker = new EmbeddedGearmanWorker(server);

        TestFunction testFunction = new TestFunction();
        worker.registerCallback("test", testFunction);

        Thread workerThread = new Thread(worker);
        workerThread.start();

        byte[] data = {'4','2','1','9','3','5','8','7'};
        byte[] data2 = {'4','9','1','9','3','5','8','6'};

        try {
            byte[] result = client.submitJob("test", data);
            System.err.println("Data: " + new String(data));
            System.err.println("Result: " + new String(result));

            Thread.sleep(500);

            byte[] result2 = client.submitJob("test", data2);
            System.err.println("Data: " + new String(data2));
            System.err.println("Result: " + new String(result2));

            Calendar c = Calendar.getInstance();
            c.add(Calendar.SECOND, 10);
            Date whenToRun = c.getTime();
            String jobHandle = client.submitFutureJob("test", data2, whenToRun);
            System.err.println("Job handle for future job: " + jobHandle);
            Thread.sleep(45000);
            System.err.println("Job should be complete...");

            worker.stopWork();
            workerThread.join();
        } catch (JobSubmissionException e) {
            e.printStackTrace();
        } catch (WorkException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    class TestFunction implements GearmanFunction {
        @Override
        public byte[] process(WorkEvent workEvent) {
            Job job = workEvent.job;
            byte[] datacopy = Arrays.copyOf(job.getData(), job.getData().length);
            Arrays.sort(datacopy);
            return datacopy;
        }
    }

    public static void main(String ... args) {
        new EmbeddedServerDemo();
    }
}
