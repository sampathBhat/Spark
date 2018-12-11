package com.spark.aggr.cep.stream;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HdfsClient {
	private static HdfsClient hdfsClient;
	private FileSystem fs;

	private HdfsClient(String hdfsUri) {
		Configuration conf = new Configuration();
		conf.set(HdfsConfigParam.HDFS_URI, hdfsUri);
		conf.set(HdfsConfigParam.HDFS_NAME, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set(HdfsConfigParam.FS_NAME, org.apache.hadoop.fs.LocalFileSystem.class.getName());

		System.setProperty(HdfsConfigParam.HDFS_USER, "root");
		System.setProperty(HdfsConfigParam.HDFS_HOMEDIR, File.separator);

		try {
			fs = FileSystem.get(URI.create(hdfsUri), conf);
			System.out.println("Configured FileSystem = " + conf.get(HdfsConfigParam.HDFS_URI));
		} catch (Exception e) {
			System.out.println("The Exception is " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	public static HdfsClient getInstance()  {
		return hdfsClient;
	}

	public static HdfsClient getInstance(final String hdfsUri) throws Exception {
		synchronized (HdfsClient.class) {
			if (hdfsClient == null) {
				hdfsClient = new HdfsClient(hdfsUri);
			}
		}
		return hdfsClient;
	}

	public FileSystem getHdfsFileSystem() {
		return fs;
	}
}