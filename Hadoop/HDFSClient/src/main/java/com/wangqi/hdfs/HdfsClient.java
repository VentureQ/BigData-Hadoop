package com.wangqi.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author wangqi
 * @version 1.0
 * @date 2021/9/28 16:22
 */
public class HdfsClient {

    private FileSystem fs;

    public static void main(String[] args) {

    }

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://hadoop102:8020");
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        String user = "wangqi";

        fs = FileSystem.get(uri, configuration, user);


    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void testmkdir() throws IOException {

        fs.mkdirs(new Path("/xiyou/huaguoshan1"));

    }

    @Test
    public void testPut() throws IOException {

        fs.copyFromLocalFile(false, true, new Path("D:\\houzi1.xlsx"), new Path("/xiyou/huaguoshan"));

    }

    @Test
    public void testGet() throws IOException {

        fs.copyToLocalFile(false, new Path("hdfs://hadoop102/xiyou/huaguoshan"), new Path("D:\\sunwukong"), true);

    }

    //获取文件详情
    @Test
    public void fileDetail() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus= listFiles.next();
            System.out.println("============"+fileStatus.getPath()+"==============");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            //获取块信息
            BlockLocation[]blockLocations=fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }
}
