package org.gearman.example;

import org.gearman.client.GearmanClient;
import org.gearman.constants.JobPriority;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 11:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class ClientDemo {
    public static void main(String... args)
    {
        try {
            byte data[] = "This is a test".getBytes();
            GearmanClient client = new GearmanClient("localhost", 4730);
            client.submitJobInBackground("foonarf", data, JobPriority.NORMAL);
        } catch (IOException ioe) {
            System.err.println("Couldn't connect: " + ioe);
        }
    }
}
