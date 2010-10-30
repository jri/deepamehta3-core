package de.deepamehta.core.util;

import java.security.MessageDigest;
import java.security.Provider;
import java.security.Security;

import java.io.UnsupportedEncodingException;

import java.net.FileNameMap;
import java.net.URLConnection;
import java.net.URLEncoder;



public class JavaUtils {

    private static FileNameMap fileTypeMap = URLConnection.getFileNameMap();

    public static String getFileType(String fileName) {
        String fileType = fileTypeMap.getContentTypeFor(fileName);
        if (fileType != null) {
            return fileType;
        }
        // fallback
        String extension = getExtension(fileName);
        if (extension.equals("mp3")) {
            return "audio/mpeg";
        }
        //
        return null;
    }

    public static String getExtension(String fileName) {
        return fileName.substring(fileName.lastIndexOf(".") + 1).toLowerCase();
    }

    public static String encodeURIComponent(String uriComp) throws UnsupportedEncodingException {
        return URLEncoder.encode(uriComp, "UTF-8").replaceAll("\\+", "%20");
    }

    // ---

    /* static {
        for (Provider p : Security.getProviders()) {
            System.out.println("### Security Provider " + p);
            for (Provider.Service s : p.getServices()) {
                System.out.println("        " + s);
            }
        }
    } */

    public static String encodeSHA256(String data) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");       // java.security.NoSuchAlgorithmException
            return new String(encodeHex(sha256.digest(data.getBytes("UTF-8")))); // java.io.UnsupportedEncodingException
        } catch (Throwable e) {
            throw new RuntimeException("Error while SHA256 encoding", e);
        }
    }

    private static char[] encodeHex(byte[] data) {
        final String DIGITS = "0123456789abcdef";
        int l = data.length;
        char[] out = new char[l << 1];
        // two characters form the hex value.
        for (int i = 0, j = 0; i < l; i++) {
            out[j++] = DIGITS.charAt((0xF0 & data[i]) >>> 4);
            out[j++] = DIGITS.charAt(0x0F & data[i]);
        }
        return out;
    }
}
