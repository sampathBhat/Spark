package com.spark.aggr.cep.batch;

public class HdfsFileDelete extends Thread {
	private CepHdfsFileWriter cepHdfsFileWriter;
	private String path;
	private String timestamp;

	public HdfsFileDelete(CepHdfsFileWriter cepHdfsFileWriter, String path, String timestamp) {
		this.cepHdfsFileWriter = cepHdfsFileWriter;
		this.path = path;
		this.timestamp = timestamp;
	}

	@Override
	public void run() {
		try {
			cepHdfsFileWriter.deleteFiles(path, timestamp);
		} catch (Throwable t) {
			t.printStackTrace();
			System.out.println("The Exception is " + t.getMessage());
		}
	}
}
