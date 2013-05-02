package org.gearman.server.web;

import java.util.HashMap;
import java.util.Map;

public class DateFormatter {
    public final static long ONE_MILLISECOND = 1;
    public final static long MILLISECONDS_IN_A_SECOND = 1000;

    public final static long ONE_SECOND = 1000;
    public final static long SECONDS_IN_A_MINUTE = 60;

    public final static long ONE_MINUTE = ONE_SECOND * 60;
    public final static long MINUTES_IN_AN_HOUR = 60;

    public final static long ONE_HOUR = ONE_MINUTE * 60;
    public final static long HOURS_IN_A_DAY = 24;
    public final static long ONE_DAY = ONE_HOUR * 24;
    public final static long DAYS_IN_A_YEAR = 365;

    public static TimeMap buildTimeMap(Number n) {
        TimeMap res = null;

        if (n != null) {
            long duration = n.longValue();

            duration /= ONE_MILLISECOND;
            int milliseconds = (int) (duration % MILLISECONDS_IN_A_SECOND);
            duration /= ONE_SECOND;
            int seconds = (int) (duration % SECONDS_IN_A_MINUTE);
            duration /= SECONDS_IN_A_MINUTE;
            int minutes = (int) (duration % MINUTES_IN_AN_HOUR);
            duration /= MINUTES_IN_AN_HOUR;
            int hours = (int) (duration % HOURS_IN_A_DAY);
            duration /= HOURS_IN_A_DAY;
            int days = (int) (duration % DAYS_IN_A_YEAR);
            duration /= DAYS_IN_A_YEAR;
            int years = (int) (duration);

            res = new TimeMap(milliseconds, seconds, minutes, hours, days, years);
        }
        return res;

    }
}

class TimeMap {
    public final int MSEC, SEC, MINUTES, HOURS, DAYS, YEARS;

    public TimeMap(int msec, int sec, int minutes, int hours, int days, int years)
    {
        MSEC = msec;
        SEC = sec;
        MINUTES = minutes;
        HOURS = hours;
        DAYS = days;
        YEARS = years;
    }
}