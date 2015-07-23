package net.johnewart.gearman.engine.exceptions;

public class PersistenceException extends Exception
{
    private final String message;

    public PersistenceException(String message)
    {
        this.message = message;
    }

    @Override
    public String getMessage()
    {
        return message;
    }
}
