package net.johnewart.gearman.server.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class GearmanGraphiteReporterConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(GearmanGraphiteReporterConfiguration.class);

    private Integer port;
    private Long period;
    private String host;
    private String prefix;
    private TimeUnit durations;
    private TimeUnit rates;

    public String getHost() {
        return host == null ? "localhost" : host;
    }

    @SuppressWarnings("unused")
    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port == null ? 2003 : port;
    }

    @SuppressWarnings("unused")
    public void setPort(int port) {
        this.port = port;
    }

    public String getPrefix() {
        return prefix == null ? "" : prefix;
    }

    @SuppressWarnings("unused")
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public TimeUnit getRates() {
        return rates == null ? TimeUnit.SECONDS : rates;
    }

    @SuppressWarnings("unused")
    public void setRates(String rates) {
        if (rates != null) {
            try {
                this.rates = TimeUnit.valueOf(rates.toUpperCase());
            } catch (IllegalArgumentException e) {
                LOG.warn("Got illegal rates value : {}", rates);
            }
        }
    }

    public TimeUnit getDurations() {
        return durations == null ? TimeUnit.SECONDS : durations;
    }

    @SuppressWarnings("unused")
    public void setDurations(String durations) {
        if (durations != null) {
            try {
                this.durations = TimeUnit.valueOf(durations.toUpperCase());
            } catch (IllegalArgumentException e) {
                LOG.warn("Got illegal durations value : {}", durations);
            }
        }
    }

    public long getPeriod() {
        return period == null ? 60 : period;
    }

    @SuppressWarnings("unused")
    public void setPeriod(long period) {
        this.period = period;
    }
}
