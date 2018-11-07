package com.saf.mllib.core.common.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils {
    public static String getValue(String fileName, String name) {
        Properties prop = new Properties();
        InputStream in = PropertiesUtils.class.getResourceAsStream(fileName);
        try {
            prop.load(in);
            return prop.getProperty(name).trim();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
