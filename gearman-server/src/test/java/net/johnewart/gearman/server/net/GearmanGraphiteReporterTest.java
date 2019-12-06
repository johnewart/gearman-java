package net.johnewart.gearman.server.net;

import net.johnewart.gearman.server.config.GearmanGraphiteReporterConfiguration;
import net.johnewart.gearman.server.config.GearmanServerConfiguration;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by genus on 08.12.2016.
 */
public class GearmanGraphiteReporterTest {

    @Test
    public void disabled() throws Exception {
        final GearmanServerConfiguration configuration = new GearmanServerConfiguration();
        final GearmanGraphiteReporter gearmanGraphiteReporter = new GearmanGraphiteReporter(configuration);

        assertFalse(gearmanGraphiteReporter.isEnabled());
    }

    @Test
    public void enabled() throws Exception {
        final GearmanServerConfiguration configuration = new GearmanServerConfiguration();
        configuration.setGraphite(new GearmanGraphiteReporterConfiguration());
        final GearmanGraphiteReporter gearmanGraphiteReporter = new GearmanGraphiteReporter(configuration);

        assertTrue(gearmanGraphiteReporter.isEnabled());

    }
}
