package org.gearman.server.web;

public class NumberFormatter
{
    public NumberFormatter() {}
    public String format(Number number) {
        long longVal = number.longValue();
        if(longVal < 1000)
        {
            return number.toString();
        }

        if (longVal < 1000000)
        {
            return String.format("%.1fK", number.doubleValue() / 1000.0);
        }

        if (longVal < 1000000000)
        {
            return String.format("%.2M", number.doubleValue() / 1000000.0);
        }

        return String.format("%.3B", number.doubleValue() / 1000000000.0);
    }
}