package com.study.hello.bigdata.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

/**
 * @author : fangxiangqian
 * @created : 12/22/2023
 **/
public class TestHdfs {

    FileSystem fs = null;
    Logger logger = LoggerFactory.getLogger(TestHdfs.class);


    @Before
    public void init() {
        {
            try {

                // 方式一
                //fs = FileSystem.get(new URI("hdfs://hadoop01.hellobigdata.com:9000"), new Configuration(), "hadoop");


                // 方式二
                Configuration conf = new Configuration();
                // 确保 Hadoop 的配置文件（core-site.xml, hdfs-site.xml）位于 classpath 中
                conf.addResource(new Path("D:\\Workspace\\StudyWorkspace\\JavaProjects\\hello-bigdata\\hello-hadoop\\src\\main\\resources\\core-site.xml"));
                conf.addResource(new Path("D:\\Workspace\\StudyWorkspace\\JavaProjects\\hello-bigdata\\hello-hadoop\\src\\main\\resources\\hdfs-site.xml"));

                String ticketCachePath = conf.get("hadoop.security.kerberos.ticket.cache.path");
                UserGroupInformation ugi = UserGroupInformation.getBestUGI(ticketCachePath, "hadoop");
                try {
                    fs = (FileSystem) ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                        public FileSystem run() throws IOException {
                            return FileSystem.get(conf);
                        }
                    });
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @After
    public void close() {
        try {
            fs.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testHdfsMkdir() {
        try {
            logger.info("testHdfsMkdir");
            boolean result = fs.mkdirs(new org.apache.hadoop.fs.Path("/test2"));
            logger.info("result:{}", result);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 参数优先级(从高到低)
     * 1.客户端代码中设置的值
     * 2.ClassPath 下的用户自定义配置文件
     * 3.服务器hdfs-site.xml中的值
     * 4.服务器hadoop默认配置hdfs-default.xml中的值
     */
    @Test
    public void testPut() {
        try {
            logger.info("testPut");
            fs.copyFromLocalFile(false, false, new Path("D:\\Workspace\\StudyWorkspace\\JavaProjects\\hello-bigdata\\hello-hadoop\\src\\main\\resources\\core-site.xml"), new Path("/test2"));
            fs.copyFromLocalFile(false, false, new Path("D:\\Workspace\\DataWorkspace\\Hadoop\\input\\word.txt"), new Path("/input"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testGet() {
        try {
            logger.info("testGet");
            fs.copyToLocalFile(false, new Path("/test2/core-site.xml"), new Path("D:\\Workspace\\StudyWorkspace\\JavaProjects\\hello-bigdata\\hello-hadoop\\src\\main\\resources\\core-site2.xml"), false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDelete() {
        try {
            logger.info("testDelete file");
            fs.delete(new Path("/test2/core-site.xml"), false);
            logger.info("testDelete dir");
            fs.delete(new Path("/test"), false);
            logger.info("testDelete non empty dir");
            fs.delete(new Path("/test2"), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRename() {
        try {
            logger.info("testRename file");
            fs.rename(new Path("/test2/core-site.xml"), new Path("/test2/core-site2.xml"));
            logger.info("testRename dir");
            fs.rename(new Path("/test2"), new Path("/test3"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testListFiles() {
        try {
            logger.info("testListFiles");
            RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), true);
            while (locatedFileStatusRemoteIterator.hasNext()) {
                LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
                logger.info("{},{}", next.getPath(), next.getOwner());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testListStatus() {
        try {
            logger.info("testListStatus");
            org.apache.hadoop.fs.FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
            for (org.apache.hadoop.fs.FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isDirectory()) {
                    logger.info("dir:{}", fileStatus.getPath());
                } else {
                    logger.info("file:{}", fileStatus.getPath());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
