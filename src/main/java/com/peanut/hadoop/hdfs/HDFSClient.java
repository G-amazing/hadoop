package com.peanut.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {

    /**
     * 文件上传
     */
    @Test
    public void upFile() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication","2");
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.245.128:9000"), configuration, "root");
        fs.copyFromLocalFile(new Path("D:\\hadoopTest\\input\\hello.txt"), new Path("/user/input/hello.txt"));
        fs.close();
    }

    /**
     * 文件上传
     */
    @Test
    public void downFile() throws IOException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.245.128:9000"), configuration, "root");
        fs.copyToLocalFile(new Path("/user/input/hello.txt"), new Path("D:\\hadoopTest\\input\\hello.txt"));
        fs.close();
    }

    /**
     * 文件删除
     */
    @Test
    public void deleteFile() throws IOException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.245.128:9000"), configuration, "root");
        fs.delete(new Path("/user/input/a.txt"), true);
        fs.close();
    }

    /**
     * 文件改名
     */
    @Test
    public void renameFile() throws IOException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.153.128:9000"), configuration, "root");
        fs.rename(new Path("/ddddd.txt"), new Path("/dd.txt"));
        fs.close();
    }

    /**
     * 查看文件详细信息
     */
    @Test
    public void getFileDetail() throws IOException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.153.128:9000"), configuration, "root");
        RemoteIterator<LocatedFileStatus> list = fs.listFiles(new Path("/"), true);
        while (list.hasNext()) {
            LocatedFileStatus fileStatus = list.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("-----------------");
        }
        fs.close();
    }

    /**
     * 判断是文件还是文件夹
     */
    @Test
    public void test() throws IOException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.153.128:9000"), configuration, "root");
        fs.rename(new Path("/ddddd.txt"), new Path("/dd.txt"));
        fs.close();
    }
}
