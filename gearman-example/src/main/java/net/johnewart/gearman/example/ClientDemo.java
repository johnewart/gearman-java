package net.johnewart.gearman.example;

import net.johnewart.gearman.client.NetworkGearmanClient;
import net.johnewart.gearman.common.JobStatus;
import net.johnewart.gearman.common.events.GearmanClientEventListener;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.exceptions.NoServersAvailableException;
import net.johnewart.gearman.exceptions.WorkException;
import net.johnewart.gearman.exceptions.WorkExceptionException;
import net.johnewart.gearman.exceptions.WorkFailException;

import java.io.IOException;

public class ClientDemo {
    private ClientDemo() { }

    public static void main(String... args)
    {
        GearmanClientEventListener eventListener = new GearmanClientEventListener() {
            @Override
            public void handleWorkData(String jobHandle, byte[] data) {
                System.err.println("Received data update for job " + jobHandle);
            }

            @Override
            public void handleWorkWarning(String jobHandle, byte[] warning) {
                System.err.println("Received warning for job " + jobHandle);
            }

            @Override
            public void handleWorkStatus(String jobHandle, JobStatus jobStatus) {
                System.err.println("Received status update for job " + jobHandle);
                System.err.println("Status: " + jobStatus.getNumerator() + " / " + jobStatus.getDenominator());
            }
        };

        try {
            byte data[] = "This is a test".getBytes();
            NetworkGearmanClient client = new NetworkGearmanClient("localhost", 4730);
            client.addHostToList("localhost", 4731);
            client.registerEventListener(eventListener);

            while(true)
            {
                try {
                    byte[] result = client.submitJob("reverse", data, JobPriority.NORMAL);
                    System.err.println("Result: " + new String(result));
                } catch (WorkException e) {
                    if(e instanceof WorkFailException)
                        System.err.println("Job " + e.getJobHandle() + " failed.");
                    else
                        System.err.println("Job " + e.getJobHandle() + " exception: " + ((WorkExceptionException) e).getMessage());

                    e.printStackTrace();
                }

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException ioe) {
            System.err.println("Couldn't connect: " + ioe);
        } catch (NoServersAvailableException nsae) {
            System.err.println("Can't connect to any servers.");
        }
    }
}
