package ch.qos.logback.core.rolling;

import ch.qos.logback.core.rolling.aws.AmazonS3Client;
import ch.qos.logback.core.rolling.shutdown.RollingPolicyShutdownListener;
import ch.qos.logback.core.rolling.shutdown.ShutdownHookType;
import ch.qos.logback.core.rolling.shutdown.ShutdownHookUtil;

import java.util.Date;

public class S3FixedWindowRollingPolicy extends FixedWindowRollingPolicy implements RollingPolicyShutdownListener {

    private String awsAccessKey;
    private String awsSecretKey;
    private String s3BucketName;
    private String s3FolderName;
    private ShutdownHookType shutdownHookType;
    private boolean rolloverOnExit;
    private boolean prefixTimestamp;
    private boolean prefixIdentifier;

    private AmazonS3Client s3Client;

    public S3FixedWindowRollingPolicy() {
        super();

        setAwsAccessKey(null);
        setAwsSecretKey(null);
        setS3FolderName(null);
        setS3BucketName(null);

        setRolloverOnExit(false);
        setPrefixTimestamp(false);
        setPrefixIdentifier(false);
        setShutdownHookType(ShutdownHookType.NONE);
    }

    @Override
    public void start() {
        super.start();

        this.s3Client = new AmazonS3Client(getAwsAccessKey(), getAwsSecretKey(), getS3BucketName(),
                getS3FolderName(), isPrefixTimestamp(), isPrefixIdentifier());

        if (isPrefixIdentifier()) {
            addInfo("Using identifier prefix \"" + this.s3Client.getIdentifier() + "\"");
        }

        ShutdownHookUtil.registerShutdownHook(this, getShutdownHookType());
    }

    @Override
    public void rollover() throws RolloverFailure {
        super.rollover();

        this.s3Client.uploadFileToS3Async(fileNamePattern.convertInt(getMinIndex()), new Date());
    }

    @Override
    public void doShutdown() {
        if (isRolloverOnExit()) {
            rollover();
        } else {
            this.s3Client.uploadFileToS3Async(getActiveFileName(), new Date(), true);
        }

        this.s3Client.doShutdown();
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
