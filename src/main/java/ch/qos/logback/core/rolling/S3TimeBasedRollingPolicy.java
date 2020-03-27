package ch.qos.logback.core.rolling;

import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.rolling.aws.AmazonS3Client;
import ch.qos.logback.core.rolling.shutdown.RollingPolicyShutdownListener;
import ch.qos.logback.core.rolling.shutdown.ShutdownHookType;
import ch.qos.logback.core.rolling.shutdown.ShutdownHookUtil;

import java.io.File;
import java.util.Date;
import java.util.concurrent.*;

public class S3TimeBasedRollingPolicy<E> extends TimeBasedRollingPolicy<E> implements RollingPolicyShutdownListener {

    private String awsAccessKey;
    private String awsSecretKey;
    private String s3BucketName;
    private String s3FolderName;
    private ShutdownHookType shutdownHookType;
    private boolean rolloverOnExit;
    private boolean prefixTimestamp;
    private boolean prefixIdentifier;

    private AmazonS3Client s3Client;
    private ExecutorService executor;

    private Date lastPeriod;

    public S3TimeBasedRollingPolicy() {
        super();

        setAwsAccessKey(null);
        setAwsSecretKey(null);
        setS3FolderName(null);
        setS3BucketName(null);

        setRolloverOnExit(false);
        setPrefixTimestamp(false);
        setPrefixIdentifier(false);
        setShutdownHookType(ShutdownHookType.NONE);

        this.lastPeriod = new Date();
        this.executor = Executors.newFixedThreadPool(1);
    }

    @Override
    public void start() {
        super.start();

        this.lastPeriod = getLastPeriod();

        this.s3Client = new AmazonS3Client(getAwsAccessKey(), getAwsSecretKey(), getS3BucketName(),
                getS3FolderName(), isPrefixTimestamp(), isPrefixIdentifier());

        if (isPrefixIdentifier()) {
            addInfo("Using identifier prefix \"" + this.s3Client.getIdentifier() + "\"");
        }

        ShutdownHookUtil.registerShutdownHook(this, getShutdownHookType());
    }

    @Override
    public void rollover() throws RolloverFailure {
        if (timeBasedFileNamingAndTriggeringPolicy.getElapsedPeriodsFileName() != null) {
            final String elapsedPeriodsFileName = String.format("%s%s", timeBasedFileNamingAndTriggeringPolicy.getElapsedPeriodsFileName(),
                    getFileNameSuffix());

            super.rollover();

            this.executor.execute(new UploadQueuer(elapsedPeriodsFileName, this.lastPeriod));
        } else {
            this.s3Client.uploadFileToS3Async(getActiveFileName(), this.lastPeriod, true);
        }
    }

    public Date getLastPeriod() {
        Date lastPeriod = ((TimeBasedFileNamingAndTriggeringPolicyBase<E>) timeBasedFileNamingAndTriggeringPolicy).dateInCurrentPeriod;
        if (getParentsRawFileProperty() != null) {
            File file = new File(getParentsRawFileProperty());
            if (file.exists() && file.canRead()) {
                lastPeriod = new Date(file.lastModified());
            }
        }
        return lastPeriod;
    }

    @Override
    public void doShutdown() {
        if (isRolloverOnExit()) {
            rollover();
        } else {
            this.s3Client.uploadFileToS3Async(getActiveFileName(), this.lastPeriod, true);
        }

        try {
            this.executor.shutdown();
            this.executor.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            this.executor.shutdownNow();
        }

        this.s3Client.doShutdown();
    }

    private void waitForAsynchronousJobToStop(Future<?> aFuture, String jobDescription) {
        if (aFuture != null) {
            try {
                aFuture.get(CoreConstants.SECONDS_TO_WAIT_FOR_COMPRESSION_JOBS, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                addError("Timeout while waiting for " + jobDescription + " job to finish", e);
            } catch (Exception e) {
                addError("Unexpected exception while waiting for " + jobDescription + " job to finish", e);
            }
        }
        this.lastPeriod = getLastPeriod();
    }

    private String getFileNameSuffix() {
        switch (compressionMode) {
            case GZ:
                return ".gz";
            case ZIP:
                return ".zip";
            case NONE:
            default:
                return "";
        }
    }

    class UploadQueuer implements Runnable {
        private final String elapsedPeriodsFileName;
        private final Date date;

        public UploadQueuer(final String elapsedPeriodsFileName, final Date date) {
            this.elapsedPeriodsFileName = elapsedPeriodsFileName;
            this.date = date;
        }

        @Override
        public void run() {
            try {
                waitForAsynchronousJobToStop(compressionFuture, "compression");
                waitForAsynchronousJobToStop(cleanUpFuture, "clean-up");
                s3Client.uploadFileToS3Async(elapsedPeriodsFileName, date);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public void setAwsAccessKey(String awsAccessKey) {
        this.awsAccessKey = awsAccessKey;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public void setAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
    }

    public String getS3BucketName() {
        return s3BucketName;
    }

    public void setS3BucketName(String s3BucketName) {
        this.s3BucketName = s3BucketName;
    }

    public String getS3FolderName() {
        return s3FolderName;
    }

    public void setS3FolderName(String s3FolderName) {
        this.s3FolderName = s3FolderName;
    }

    public boolean isRolloverOnExit() {
        return rolloverOnExit;
    }

    public void setRolloverOnExit(boolean rolloverOnExit) {
        this.rolloverOnExit = rolloverOnExit;
    }

    public ShutdownHookType getShutdownHookType() {
        return shutdownHookType;
    }

    public void setShutdownHookType(ShutdownHookType shutdownHookType) {
        this.shutdownHookType = shutdownHookType;
    }

    public boolean isPrefixTimestamp() {
        return prefixTimestamp;
    }

    public void setPrefixTimestamp(boolean prefixTimestamp) {
        this.prefixTimestamp = prefixTimestamp;
    }

    public boolean isPrefixIdentifier() {
        return prefixIdentifier;
    }

    public void setPrefixIdentifier(boolean prefixIdentifier) {
        this.prefixIdentifier = prefixIdentifier;
    }
}
