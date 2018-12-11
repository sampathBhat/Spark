package com.spark.aggr.cep.batch;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkQuery extends Thread {

	private final SparkSession spark;
	private final long startTime;
	private final long endTime;
	private final String srcpath;
	private final CepHdfsFileWriter cepHdfsFileWriter;
	private final AggregationTypes aggregationTypes;

	public SparkQuery(final SparkSession spark, final long startTime, final long endTime, final String srcpath,
			final CepHdfsFileWriter cepHdfsFileWriter, final AggregationTypes aggregationTypes) {
		this.spark = spark;
		this.startTime = startTime;
		this.endTime = endTime;
		this.srcpath = srcpath;
		this.cepHdfsFileWriter = cepHdfsFileWriter;
		this.aggregationTypes = aggregationTypes;

	}

	@Override
	public void run() {
		try {
			final long sparkStartTime = System.currentTimeMillis();
			// Read all the JSON files inside the HDFS nested input folder
			Dataset<Row> ds = spark.read().json(srcpath + File.separator + "*");
			String query = null;
			// Create a table view with the input JSON
			ds.createOrReplaceTempView("netflowv5");
			if (aggregationTypes == AggregationTypes.NIFI) {
				query = "SELECT CONCAT(STARTTIME,'-',ENDTIME) as TimeStamp, srcaddr as src_ip, srcport as src_port,count(dOctets) as dOctetsCount, COALESCE(sum(dOctets),0) as dOctetsSum from netflowv5 where unix_secs >= STARTTIME and unix_secs < ENDTIME group by srcaddr, srcport";
			} else if (aggregationTypes == AggregationTypes.IGNITE) {
				query = "SELECT CONCAT(STARTTIME,'-',ENDTIME) as TimeStamp, srcaddr as src_ip, srcport as src_port,sum(countOctets) as dOctetsCount, COALESCE(sum(sumOctets),0) as dOctetsSum from netflowv5 where timestamp >= STARTTIME and timestamp < ENDTIME group by srcaddr, srcport";
			}
			query = query.replaceAll("STARTTIME", String.valueOf(startTime));
			query = query.replaceAll("ENDTIME", String.valueOf(endTime));

			Dataset<Row> result = spark.sql(query);

			final long sparkEndTime = System.currentTimeMillis();
			System.out.println("STARTTIME  = " + startTime + "  ENDTIME  = " + endTime + " Spark Query Time  =  "
					+ (sparkEndTime - sparkStartTime));

			result.show();
			result.toJSON().collectAsList().forEach(s -> System.out.println("The result set is  " + s));

			StringBuilder sb = new StringBuilder("");
			result.toJSON().collectAsList().forEach(s -> sb.append(s + "\n"));
			if (!(sb.toString().equals(""))) {
				cepHdfsFileWriter.writeLineToHdfs(sb.toString().trim());
			}

		} catch (Throwable t) {
			t.printStackTrace();
			System.out.println("The Exception is " + t.getMessage());
		}
	}
}
