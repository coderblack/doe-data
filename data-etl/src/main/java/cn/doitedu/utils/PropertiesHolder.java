package cn.doitedu.utils;

import java.io.IOException;
import java.util.Properties;

public class PropertiesHolder {

    private static Properties props = null;

    public static String getProperty(String key) throws IOException {
        if(props == null) {
            props = new Properties();
            props.load(PropertiesHolder.class.getClassLoader().getResourceAsStream("user_profile_tags_bulkload.properties"));
        }

        return props.getProperty(key);

    }
}
