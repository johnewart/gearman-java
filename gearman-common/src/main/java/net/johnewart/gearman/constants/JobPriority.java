package net.johnewart.gearman.constants;

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
