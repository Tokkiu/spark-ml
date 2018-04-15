package cn.heio.spark.ml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Util {
    public static Properties getProperties(String path) throws IOException {
        Properties properties = new Properties();
        try(InputStream inputStream = new FileInputStream(path)){
            properties.load(inputStream);
        }

        return properties;
    }
}
