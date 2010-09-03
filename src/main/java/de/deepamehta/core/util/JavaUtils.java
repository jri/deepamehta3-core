package de.deepamehta.core.util;

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
}
