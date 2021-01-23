package com.github.zjjfly.hr;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * 运行hadoop com.github.zjjfly.hr.UrlCat /input/sample.txt
 *
 * @author zjjfly[https://github.com/zjjfly] on 2021/1/21
 */
public class UrlCat {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws IOException {
        InputStream inputStream = null;
        try {
            inputStream = new URL(Constants.HDFS_URI_STR + args[0]).openStream();
            IOUtils.copyBytes(inputStream, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(inputStream);
        }
    }
}
