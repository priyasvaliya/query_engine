package Project.login;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashAlgorithmUtil {
    public static String getSHA256Hash(String source) {
        if (source == null) {
            return null;
        }
        try {
            final MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
            return String.format("%064x", new BigInteger(1, messageDigest.digest(source.getBytes(StandardCharsets.UTF_8))));
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }

    public static boolean validateSHA256Hash(String source, String targetHash) {
        final String sourceHash = getSHA256Hash(source);
        if (sourceHash == null || targetHash == null) {
            return false;
        }
        return sourceHash.equals(targetHash);
    }
}
