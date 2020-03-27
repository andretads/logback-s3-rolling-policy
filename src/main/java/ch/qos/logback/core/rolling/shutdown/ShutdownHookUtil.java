package ch.qos.logback.core.rolling.shutdown;

public class ShutdownHookUtil {

    public static void registerShutdownHook(RollingPolicyShutdownListener listener, ShutdownHookType shutdownHookType) {
        if (shutdownHookType == null) {
            shutdownHookType = ShutdownHookType.NONE;
        }

        switch (shutdownHookType) {
            case SERVLET_CONTEXT:
                RollingPolicyContextListener.registerShutdownListener(listener);
                break;
            case JVM_SHUTDOWN_HOOK:
                Runtime.getRuntime().addShutdownHook(new Thread(new RollingPolicyJVMListener(listener)));
                break;
            case NONE:
            default:
                break;
        }
    }
}
