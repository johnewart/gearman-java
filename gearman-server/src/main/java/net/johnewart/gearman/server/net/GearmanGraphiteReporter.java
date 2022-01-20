package net.johnewart.gearman.server.net;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import net.johnewart.gearman.server.config.GearmanGraphiteReporterConfiguration;
import net.johnewart.gearman.server.config.GearmanServerConfiguration;

import java.util.concurrent.TimeUnit;

public class GearmanGraphiteReporter {

    private final boolean enabled;
    private final GraphiteReporter reporter;
    private final long period;

    public GearmanGraphiteReporter(GearmanServerConfiguration configuration) {
        GearmanGraphiteReporterConfiguration graphiteConfiguration = configuration.getGraphite();
        if (graphiteConfiguration != null) {
            enabled = true;
            final Graphite graphite = new Graphite(graphiteConfiguration.getHost(), graphiteConfiguration.getPort());
            period = graphiteConfiguration.getPeriod();

            reporter = GraphiteReporter.forRegistry(configuration.getMetricRegistry())
                    .prefixedWith(graphiteConfiguration.getPrefix())
                    .convertRatesTo(graphiteConfiguration.getRates())
                    .convertDurationsTo(graphiteConfiguration.getDurations())
                    .filter(MetricFilter.ALL)
                    .build(graphite);
        } else {
            reporter = null;
            period = 0L;
            enabled = false;
        }
    }

    public void start() {
        if (enabled) {
            reporter.start(period, TimeUnit.SECONDS);
        }
    }

    boolean isEnabled() {
        return enabled;
    }
}
