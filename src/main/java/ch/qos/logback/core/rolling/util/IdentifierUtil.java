package ch.qos.logback.core.rolling.util;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.UUID;
import java.util.logging.Logger;

public class IdentifierUtil {

    private static final Logger logger = Logger.getLogger(IdentifierUtil.class.getSimpleName());

    public static String getIdentifier() {
        String identifier = getContentOfWebpage("http://169.254.169.254/latest/meta-data/instance-id");
        if (identifier != null) {
            return identifier;
        }


        identifier = getHostname();
        if (identifier != null) {
            return identifier;
        }

        return UUID.randomUUID().toString();
    }

    public static String getContentOfWebpage(String location) {
        try {
            URL url = new URL(location);
            URLConnection con = url.openConnection();
            InputStream in = con.getInputStream();
            String encoding = con.getContentEncoding();
            encoding = encoding == null ? "UTF-8" : encoding;

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            int len;
            while ((len = in.read(buf)) != -1) {
                baos.write(buf, 0, len);
            }
            String body = new String(baos.toByteArray(), encoding);
            if (body.trim().length() > 0) {
                return body.trim();
            }
        } catch (Exception e) {
            logger.fine(e.getMessage());
        }
        return null;
    }

    public static String getHostname() {
        try {
            String hostname = InetAddress.getLocalHost().getHostAddress();
            if (hostname != null) {
                hostname = hostname.replaceAll("[^a-zA-Z0-9.]+", "").trim();
            }

            if (hostname != null && hostname.length() > 0) {
                return hostname;
            }
        } catch (Exception e) {
            logger.fine(e.getMessage());
        }
        return null;
    }
}
