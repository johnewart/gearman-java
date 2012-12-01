package org.gearman.server;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/14/12
 * Time: 2:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class IllegalJobStateTransitionException extends Exception {
    String message;

    public IllegalJobStateTransitionException(String message)
    {
        this.message = message;
    }

    public String toString()
    {
        return "IllegalJobStateTransitionException: " + message;
    }
}
