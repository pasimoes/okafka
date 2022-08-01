package org.oracle.okafka.connect.common.utils;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Sanitizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Properties;

public class AppInfoParser {
    private static final Logger log = LoggerFactory.getLogger(AppInfoParser.class);

    private static final String NAME;
    private static final String VERSION;
    private static final String COMMIT_ID;

    static {
        Properties props = new Properties();
        try (InputStream resourceStream = AppInfoParser.class.getResourceAsStream("/kafka-connect-oracle-version.properties")) {
            props.load(resourceStream);
        } catch (Exception e) {
            log.warn("Error while loading kafka-connect-oracle-version.properties :" + e.getMessage());
        }

        NAME = props.getProperty("name", "unknown").trim();
        VERSION = props.getProperty("version", "unknown").trim();
        COMMIT_ID = props.getProperty("commitId", "unknown").trim();
        log.trace("[{}] app-name={} version={}", Thread.currentThread().getId(), NAME, VERSION);
    }

    public static String getName() {
        return NAME;
    }
    public static String getVersion() {
        return VERSION;
    }
    public static String getCommitId() {
        return COMMIT_ID;
    }

    public static synchronized void registerAppInfo(String prefix, String id, Metrics metrics) {
        try {
            ObjectName name = new ObjectName(prefix + ":type=app-info,id=" + Sanitizer.jmxSanitize(id));
            AppInfoParser.AppInfo mBean = new AppInfoParser.AppInfo();
            ManagementFactory.getPlatformMBeanServer().registerMBean(mBean, name);

            registerMetrics(metrics); // prefix will be added later by JmxReporter
        } catch (JMException e) {
            log.warn("Error registering AppInfo mbean", e);
        }
    }

    public static synchronized void unregisterAppInfo(String prefix, String id, Metrics metrics) {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName name = new ObjectName(prefix + ":type=app-info,id=" + Sanitizer.jmxSanitize(id));
            if (server.isRegistered(name))
                server.unregisterMBean(name);

            unregisterMetrics(metrics);
        } catch (JMException e) {
            log.warn("Error unregistering AppInfo mbean", e);
        }
    }

    private static MetricName metricName(Metrics metrics, String name) {
        return metrics.metricName(name, "app-info", "Metric indicating " + name);
    }

    private static void registerMetrics(Metrics metrics) {
        if (metrics != null) {
            metrics.addMetric(metricName(metrics, "version"), new AppInfoParser.ImmutableValue<>(VERSION));
            metrics.addMetric(metricName(metrics, "commit-id"), new AppInfoParser.ImmutableValue<>(COMMIT_ID));
        }
    }

    private static void unregisterMetrics(Metrics metrics) {
        if (metrics != null) {
            metrics.removeMetric(metricName(metrics, "version"));
            metrics.removeMetric(metricName(metrics, "commit-id"));
        }
    }

    public interface AppInfoMBean {
        public String getVersion();
        public String getCommitId();
    }

    public static class AppInfo implements AppInfoParser.AppInfoMBean {

        public AppInfo() {
            String appInfoName = AppInfoParser.getName();
            log.info(appInfoName + " version : " + AppInfoParser.getVersion());
            log.info(appInfoName + " commitId : " + AppInfoParser.getCommitId());
        }
        @Override
        public String getVersion() {
            return AppInfoParser.getVersion();
        }

        @Override
        public String getCommitId() {
            return AppInfoParser.getCommitId();
        }

    }

    static class ImmutableValue<T> implements Gauge<T> {
        private final T value;

        public ImmutableValue(T value) {
            this.value = value;
        }

        @Override
        public T value(MetricConfig config, long now) {
            return value;
        }
    }
}
