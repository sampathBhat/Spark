package com.spark.aggr.cep.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;

public final class KafkaStreamProcessor {
	public static void main(String[] args) {
		final String path = args[0];
		final SparkConf sparkConfiguration = new SparkConf().setAppName("HDFSStream");
		SparkContext sc = new SparkContext(sparkConfiguration);
		StreamingContext sparkStreamContext = new StreamingContext(sc, Seconds.apply(5));
		DStream<String> rdd = sparkStreamContext.textFileStream(path);
		rdd.print();
		System.out.println("Starting Spark program");
		sparkStreamContext.start();
		System.out.println("Spark program started");
		sparkStreamContext.awaitTermination();
	}
}
