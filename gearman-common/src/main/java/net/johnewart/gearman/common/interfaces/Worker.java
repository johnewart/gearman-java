package net.johnewart.gearman.common.interfaces;

import java.util.Set;

public interface Worker {
    public Set<String> getAbilities();
    public void addAbility(String ability);
    public void wakeUp();
}
