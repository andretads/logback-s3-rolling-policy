package ch.qos.logback.core.rolling.aws;

import ch.qos.logback.core.rolling.data.CustomData;
import ch.qos.logback.core.rolling.shutdown.RollingPolicyShutdownListener;
import ch.qos.logback.core.rolling.util.IdentifierUtil;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AmazonS3Client implements RollingPolicyShutdownListener {

    private final String awsAccessKey;
    private final String awsSecretKey;
    private final String s3BucketName;
    private final String s3FolderName;

    private final boolean prefixTimestamp;
    private final boolean prefixIdentifier;

    private final String identifier;

    private ExecutorService executor;
    private AmazonS3 amazonS3;

    public AmazonS3Client(String awsAccessKey, String awsSecretKey, String s3BucketName,
                          String s3FolderName, boolean prefixTimestamp, boolean prefixIdentifier) {
        this.awsAccessKey = awsAccessKey;
        this.awsSecretKey = awsSecretKey;
        this.s3BucketName = s3BucketName;
        this.s3FolderName = s3FolderName;

        this.prefixTimestamp = prefixTimestamp;
        this.prefixIdentifier = prefixIdentifier;

        executor = Executors.newFixedThreadPool(1);
        amazonS3 = null;

        identifier = prefixIdentifier ? IdentifierUtil.getIdentifier() : null;
    }

    public void uploadFileToS3Async(final String filename, final Date date) {
        uploadFileToS3Async(filename, date, false);
    }

    public void uploadFileToS3Async(final String filename, final Date date, final boolean overrideTimestampSetting) {
        if (amazonS3 == null) {
            AWSCredentials credenciais = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            amazonS3 = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credenciais))
                    .withRegion(Regions.US_EAST_1)
                    .build();

            if (!amazonS3.doesBucketExistV2(s3BucketName)) {
                amazonS3.createBucket(new CreateBucketRequest(s3BucketName));
            }
        }

        final File file = new File(filename);
        if (!file.exists() || file.length() == 0) {
            return;
        }

        final StringBuilder s3ObjectName = new StringBuilder();
        if (getS3FolderName() != null) {
            s3ObjectName.append(format(getS3FolderName(), date)).append("/");
        }

        if (CustomData.extraS3Folder.get() != null) {
            s3ObjectName.append(CustomData.extraS3Folder.get()).append("/");
        }

        if (prefixTimestamp || overrideTimestampSetting) {
            s3ObjectName.append(new SimpleDateFormat("yyyyMMddHHmmss").format(date)).append("_");
        }

        if (prefixIdentifier) {
            s3ObjectName.append(identifier).append("_");
        }

        s3ObjectName.append(file.getName());

        Runnable uploader = () -> {
            try {
                amazonS3.putObject(
                        new PutObjectRequest(getS3BucketName(), s3ObjectName.toString(), file)
                                .withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        };

        executor.execute(uploader);
    }

    @Override
    public void doShutdown() {
        try {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    private String format(String s, Date date) {

        Pattern pattern = Pattern.compile("%d\\{(.*?)\\}");
        Matcher matcher = pattern.matcher(s);

        while (matcher.find()) {
            String match = matcher.group(1);
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(match);
            String replace = simpleDateFormat.format(date);
            s = s.replace(String.format("%%d{%s}", match), replace);
        }
        return s;
    }

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public String getS3BucketName() {
        return s3BucketName;
    }

    public String getS3FolderName() {
        return s3FolderName;
    }

    public boolean isPrefixTimestamp() {
        return prefixTimestamp;
    }

    public boolean isPrefixIdentifier() {
        return prefixIdentifier;
    }

    public String getIdentifier() {
        return identifier;
    }
}
