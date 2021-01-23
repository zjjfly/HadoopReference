package com.github.zjjfly.hr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author zjjfly[https://github.com/zjjfly] on 2021/1/21
 */
public class ShowFileStatusTest {

    private MiniDFSCluster cluster;

    private FileSystem fs;

    @Before
    public void setup() throws IOException {
        Configuration conf = new Configuration();
        if (null == System.getProperty("test.build.data")) {
            System.setProperty("test.build.data", "/tmp");
        }
        cluster = new MiniDFSCluster.Builder(conf).build();
        fs = cluster.getFileSystem();
        FSDataOutputStream out = fs.create(new Path("/dir/file"));
        out.write("content".getBytes(StandardCharsets.UTF_8));
        out.close();
    }

    @After
    public void teardown() throws IOException {
        if (fs != null) {
            fs.close();
        }
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void fileNotFound() throws IOException {
        fs.getFileStatus(new Path("/no-such-file"));
    }


}
