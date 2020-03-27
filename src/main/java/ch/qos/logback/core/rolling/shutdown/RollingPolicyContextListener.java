package ch.qos.logback.core.rolling.shutdown;

import com.google.common.collect.Lists;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.List;

public class RollingPolicyContextListener implements ServletContextListener {

    private static final List<RollingPolicyShutdownListener> listeners;

    static {
        listeners = Lists.newArrayList();
    }

    public static void registerShutdownListener(final RollingPolicyShutdownListener listener) {
        if (!listeners.contains(listener)) {
            listeners.add(listener);
        }
    }

    public static void deregisterShutdownListener(final RollingPolicyShutdownListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        for (RollingPolicyShutdownListener listener : listeners) {
            listener.doShutdown();
        }
    }
}
