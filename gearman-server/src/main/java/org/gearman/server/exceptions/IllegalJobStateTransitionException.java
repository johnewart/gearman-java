package org.gearman.server.exceptions;

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
