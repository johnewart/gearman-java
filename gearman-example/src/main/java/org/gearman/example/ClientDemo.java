package org.gearman.example;

import org.gearman.client.GearmanClient;
import org.gearman.constants.JobPriority;

import java.io.IOException;

public class ClientDemo {
    public static void main(String... args)
    {
        try {
            byte data[] = "This is a test".getBytes();
            GearmanClient client = new GearmanClient("localhost", 4730);
            client.addHostToList("localhost", 4731);
            while(true)
            {
                client.submitJobInBackground("foonarf", data, JobPriority.NORMAL);

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException ioe) {
            System.err.println("Couldn't connect: " + ioe);
        }
    }
}
