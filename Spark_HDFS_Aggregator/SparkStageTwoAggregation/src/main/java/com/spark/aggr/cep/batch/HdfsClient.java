package com.spark.aggr.cep.batch;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HdfsClient {

    private static HdfsClient hdfsClient;
    private FileSystem fs;
    
    private static final String FS_PARAM_NAME = "fs.defaultFS";
    
    private HdfsClient() throws Exception {
        Configuration conf = new Configuration();
        String hdfsuri = GeneralConfigReader.getInstance().getHdfsUriProperty();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsuri);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", GeneralConfigReader.getInstance().getHdfsUser());
        System.setProperty("hadoop.home.dir", "/");
        // Get the filesystem - HDFS
        try {
            fs = FileSystem.get(URI.create(hdfsuri), conf);
            System.out.println("configured filesystem = " + conf.get(FS_PARAM_NAME));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    
    public static HdfsClient getInstance() throws Exception {
        if (hdfsClient == null) {
            synchronized(HdfsClient.class) {
                if (hdfsClient == null) {
                    hdfsClient = new HdfsClient();
                }
            }
        }
        return hdfsClient;        
    }
    
    public FileSystem getHdfsFileSystem() {
        return fs;
    }
}
