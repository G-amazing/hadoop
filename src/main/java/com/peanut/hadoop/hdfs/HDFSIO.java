package com.peanut.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSIO {

    /**
     * 把本地e盘上的文件上传到HDFS根目录
     */
    @Test
    public void test1() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.153.128:9000"), configuration, "root");

        // 获取输入流
        FileInputStream fis = new FileInputStream(new File("C:\\Users\\GaoJie\\Desktop\\abc.txt"));

        // 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/ccc.txt"));

        // 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 把 HDFS根目录文件下载到本地磁盘上的文件
     */
    @Test
    public void test2() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.153.128:9000"), configuration, "root");

        // 获取输入流
        FSDataInputStream fis = fs.open(new Path("/ccc.txt"));

        // 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("f:/ccc.txt"));

        // 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }


}
