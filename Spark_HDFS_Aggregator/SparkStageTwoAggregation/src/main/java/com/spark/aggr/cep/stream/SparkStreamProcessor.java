package com.spark.aggr.cep.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public final class SparkStreamProcessor {
	private static final Duration SLIDE_INTERVAL = new Duration(5 * 1000);

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws InterruptedException {
		JavaStreamingContext sparkStreamContext = null;
		try {
			final String path = args[0];

			final SparkConf sparkConfiguration = new SparkConf().setAppName("HDFSStream");
			JavaSparkContext sc = new JavaSparkContext(sparkConfiguration);

			sparkStreamContext = new JavaStreamingContext(sc, SLIDE_INTERVAL);
			JavaDStream<String> windowFiles = sparkStreamContext.textFileStream(path);

			SQLContext sqlContext = new SQLContext(sc);

			windowFiles.foreachRDD(rdd -> {
				if (rdd.count() > 0) {
					Dataset<Row> ds = sqlContext.read().json(rdd);
					ds.createOrReplaceTempView("netflowv5");

					long sparkStartTime = System.currentTimeMillis() / 1000;
					long sparkEndTime = sparkStartTime + 5;
					String query = "SELECT CONCAT(" + sparkStartTime + ",'-'," + sparkEndTime
							+ ") as TimeStamp, srcaddr as src_ip, srcport as src_port,count(dOctets) as dOctetsCount, COALESCE(sum(dOctets),0) as dOctetsSum from netflowv5 group by srcaddr, srcport";

					Dataset<Row> result = sqlContext.sql(query);
					result.show();
					
					StringBuilder sb = new StringBuilder("");
					result.toJSON().collectAsList().forEach(s -> sb.append(s + "\n"));
					if (!(sb.toString().equals(""))) {
						//cepHdfsFileWriter.writeLineToHdfs(sb.toString().trim());
					}
				}
			});

			sparkStreamContext.start();
			sparkStreamContext.awaitTermination();
		} catch (Throwable t) {
			t.printStackTrace();
			System.out.println("Restarting the code... The Exception due to " + t.getMessage());
			SparkStreamProcessor.main(args);
		} finally {
			assert sparkStreamContext != null : "Null Instance Found";
			sparkStreamContext.close();
		}
	}
}
