package com.github.zjjfly.hr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

import static com.github.zjjfly.hr.Constants.HDFS_URI;

/**
 * 运行hadoop com.github.zjjfly.hr.FileSystemCat /input/sample.txt
 *
 * @author zjjfly[https://github.com/zjjfly] on 2021/1/21
 */
public class FileSystemCat {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(HDFS_URI, conf);
        InputStream inputStream = null;
        try {
            inputStream = fs.open(new Path(args[0])).getWrappedStream();
            IOUtils.copyBytes(inputStream, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(inputStream);
        }
    }
}
