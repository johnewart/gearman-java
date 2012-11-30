package org.gearman.constants;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 9:43 AM
 * To change this template use File | Settings | File Templates.
 */
public enum JobPriority {
        HIGH,
        NORMAL,
        LOW;

    public int getIndex() { return ordinal() + 1; }

    public static JobPriority fromInteger(int x) {
        switch(x) {
            case 1:  return HIGH;
            case 2:  return NORMAL;
            case 3:  return LOW;
        }
        return null;
    }
}
