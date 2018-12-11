package com.spark.aggr.cep.batch;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class CepHdfsFileWriter {

	private FileSystem fs;
	private String fileName; // this is the queryId

	private FSDataOutputStream outputStream;

	public CepHdfsFileWriter(String fileNameTmp, String dstPAth) throws Exception {
		this.fileName = fileNameTmp;
		try {
			fs = HdfsClient.getInstance().getHdfsFileSystem();
			Path newFolderPath = new Path(dstPAth);
			if (!fs.exists(newFolderPath)) {
				// Create new Directory
				fs.mkdirs(newFolderPath);
				System.out.println("Path " + fileName + " created.");
			}
			Path hdfswritepath = new Path(newFolderPath + "/" + fileName);
			// Init output stream
			if (fs.exists(hdfswritepath)) {
				// outputStream = fs.append(hdfswritepath);
				fs.delete(hdfswritepath, true);
			}
			outputStream = fs.create(hdfswritepath);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("   e.getMessage()       " + e.getMessage());
		}
		System.out.println("Created CepHdfsFileWriterActor filename : " + fileName);
	}

	public synchronized void writeLineToHdfs(String line) {
		// boolean writeStatus = false;
		try {
			if (line.contains("nullable")) {
				return;
			}
			if (outputStream == null) {
				System.out.println("======================================================================");
				System.out.println("outputStream  = " + outputStream + "    line is    " + line);
				System.out.println("======================================================================");
				return;
			}
			outputStream.writeBytes(line);
			outputStream.writeBytes("\n");
			outputStream.flush();
			outputStream.hflush();
			outputStream.hsync();
			// writeStatus = true;
			System.out.println("Wrote data to hdfs");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deleteFiles(String path, String givenName) throws Throwable {
		final long fileDeleteStartTime = System.currentTimeMillis();
		// 3. Get the metadata of the desired directory
		FileStatus[] fileStatus = fs.listStatus(new Path(path));
		int numOfFiles = fileStatus.length;
		/*
		 * if (numOfFiles < 10) {
		 * System.out.println("Less files to delete do not deleting num of files are " +
		 * numOfFiles); return; }
		 */
		String[] nameArray = new String[numOfFiles];
		for (int i = 0; i < numOfFiles; i++) {
			nameArray[i] = fileStatus[i].getPath().getName();
		}
		Arrays.parallelSort(nameArray);
		int index = getFirstMatched(nameArray, givenName);

		for (int i = 0; i < index; ++i) {
			Path deletePath = new Path(path + File.separator + nameArray[i]);
			if (fs.exists(deletePath) && nameArray[i].startsWith("data")) {
				fs.delete(deletePath, true);
				System.out.println("The file " + deletePath.toString() + "  has been deleted");
			}
		}
		final long fileDeleteEndTime = System.currentTimeMillis();
		System.out.println("STARTTIME DELETE = " + fileDeleteStartTime + " TOTALTIME DELETE = "
				+ (fileDeleteEndTime - fileDeleteStartTime));
	}

	private static int getFirstMatched(String[] filename, String pattern) {
		for (int i = 0; i < filename.length; i++) {
			if (filename[i].startsWith(pattern))
				return i;
		}
		return -1;
	}

	public List<String> listDirectories(final String path) throws Throwable {
		FileStatus[] fileStatus = fs.listStatus(new Path(path));
		List<String> subDirectories = new ArrayList<String>();
		for (FileStatus fs : fileStatus) {
			if (fs.isDirectory()) {
				subDirectories.add(fs.getPath().getName());
			}
		}
		return subDirectories;
	}
}
