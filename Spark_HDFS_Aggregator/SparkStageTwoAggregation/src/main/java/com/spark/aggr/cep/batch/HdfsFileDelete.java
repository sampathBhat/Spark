package com.spark.aggr.cep.batch;

import java.io.File;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileDelete extends Thread {
	private CepHdfsFileWriter cepHdfsFileWriter;
	private String path;
	private String timestamp;
	private FileSystem fs;
	private Path p;

	public HdfsFileDelete(CepHdfsFileWriter cepHdfsFileWriter, String path, String timestamp) throws Exception { this.cepHdfsFileWriter = cepHdfsFileWriter;
		this.path = path;
		this.timestamp = timestamp;
		this.fs = HdfsClient.getInstance().getHdfsFileSystem();
		this.p = new Path(path);
	}

	@Override
	public void run() {
		try {
			if (fs.exists(p)) {
				List<String> subDir = cepHdfsFileWriter.listDirectories(path);
				for (String dir : subDir) {
					String srcpath = path + File.separator + dir;
					System.out.println("The dir path is "+srcpath);
					cepHdfsFileWriter.deleteFiles(srcpath, timestamp);
				}
			}
		} catch (Throwable t) {
			t.printStackTrace();
			System.out.println("The Exception is " + t.getMessage());
		}
	}
}
