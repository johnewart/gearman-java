package net.johnewart.gearman.engine.exceptions;

public class EnqueueException extends Exception
{
    private final Exception inner;
    private final String message;

    public EnqueueException(Exception inner)
    {
        this.inner = inner;
        this.message = inner.getMessage();
    }

    @Override
    public String getMessage()
    {
        return message;
    }
}
