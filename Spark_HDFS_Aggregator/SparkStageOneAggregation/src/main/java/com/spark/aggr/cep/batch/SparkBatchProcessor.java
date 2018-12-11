package com.spark.aggr.cep.batch;

import java.io.File;
import java.util.List;

import org.apache.spark.sql.SparkSession;

public class SparkBatchProcessor {
	public static void main(String[] args) throws Exception {
		// int count =100;
		SparkSession spark = SparkSession.builder().appName("Stage1-Batch Processing").getOrCreate();
		try {
			// HDFS URI as mentioned in core-site xml
			String hdfsUri = args[0];
			// HDFS Input Path
			String srcpath = args[1];
			// HDFS Result Path
			String dstpath = args[2];
			// Delay Factor
			long delayFactor = Integer.parseInt(args[3]);

			long aggregationPeriod = 5L;
			GeneralConfigReader.getInstance().setHdfsUriProperty(hdfsUri);
			CepHdfsFileWriter cepHdfsFileWriter = new CepHdfsFileWriter("result.json", dstpath);
			long seconds = System.currentTimeMillis() / 1000;

			while (true) {
				long startTime = seconds - aggregationPeriod;
				long endTime = seconds;

				long currentTime = System.currentTimeMillis() / 1000;
				if (currentTime > (endTime + delayFactor)) {
					SparkQuery query = new SparkQuery(spark, startTime, endTime, srcpath, cepHdfsFileWriter);
					query.start();
					seconds += aggregationPeriod;

					long timeDelete = ((endTime - 20) / 10);
					List<String> subDir = cepHdfsFileWriter.listDirectories(srcpath);
					for (String dir : subDir) {
						String path = srcpath + File.separator + dir;
						HdfsFileDelete hdfsFileDelete = new HdfsFileDelete(cepHdfsFileWriter, path,
								"data_" + timeDelete);
						hdfsFileDelete.start();
					}
				} else {
					Thread.sleep(500);
				}
			}
		} catch (Throwable t) {
			t.printStackTrace();
			System.out.println("Restarting the code... The Exception due to " + t.getMessage());
			SparkBatchProcessor.main(args);
		} finally {
			spark.stop();
		}
	}
}