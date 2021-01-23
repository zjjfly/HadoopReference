package com.github.zjjfly.hr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import static com.github.zjjfly.hr.Constants.HDFS_URI;

/**
 * 运行hadoop com.github.zjjfly.hr.FileCopyWithProcess input/1902 /input/1
 *
 * @author zjjfly[https://github.com/zjjfly] on 2021/1/21
 */
public class FileCopyWithProcess {

    public static void main(String[] args) throws IOException {
        String localSrc = args[0];
        String dst = args[1];
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(localSrc));

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(HDFS_URI, conf);
        OutputStream out = fs.create(new Path(dst), () -> {
            System.out.print(".");
        });
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
