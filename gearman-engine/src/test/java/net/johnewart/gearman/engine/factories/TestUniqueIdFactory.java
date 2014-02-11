package net.johnewart.gearman.engine.factories;

import net.johnewart.gearman.engine.core.UniqueIdFactory;
import net.johnewart.gearman.engine.util.LocalUniqueIdFactory;

public class TestUniqueIdFactory implements UniqueIdFactory {
    private UniqueIdFactory uniqueIdFactory;

    public TestUniqueIdFactory() {
        this.uniqueIdFactory = new LocalUniqueIdFactory();
    }

    @Override
    public String generateUniqueId() {
        return uniqueIdFactory.generateUniqueId();
    }
}
