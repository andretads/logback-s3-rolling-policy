package ch.qos.logback.core.rolling.shutdown;

public class RollingPolicyJVMListener implements Runnable {

    private final RollingPolicyShutdownListener listener;

    public RollingPolicyJVMListener(final RollingPolicyShutdownListener listener) {
        this.listener = listener;
    }

    @Override
    public void run() {
        this.listener.doShutdown();
    }
}
