package com.smartlab.hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsUtils {

	/*
	 * ClassName : HdfsUtils
	 * 
	 * @Author : dengjie
	 * 
	 * 功能 : 对hadoop集群进行上传，下载，查看路径，删除。
	 */

	private static final int MAX_SIZE = 4096;

//	static String uri="hdfs://master:9000/input/input.txt";
//	static String path="/home/hadoop/input.txt";
	public static void upload(String path, String uri) {
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(path));
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			OutputStream out = fs.create(new Path(uri));
			IOUtils.copyBytes(in, out, MAX_SIZE, true);
			System.out.println("success!!!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// uri="hdfs://master:9000/hadoop"
	// path="/home/hadoop/test/hadoop1.0.3_1.tar.gz"
	public static void downloadFile(String uri, String path) {

		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), conf);

			OutputStream out = new FileOutputStream(path);
			InputStream in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, out, MAX_SIZE, false);
			System.out.println("finish!!!");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// uri="hdfs://master:9000/"
	// root="hdfs://master:9000/"
	// path="hdfs://master:9000/tmp"
	public static void listFilePath(String uri, String root, String path) {

		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path[] paths = new Path[2];
			paths[0] = new Path(root);// 根目录下的所有目录路径
			paths[1] = new Path(path);// tmp目录下的所有目录路径

			FileStatus[] status = fs.listStatus(paths);
			Path[] listPaths = FileUtil.stat2Paths(status);

			for (Path p : listPaths) {
				System.out.println(p);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// uri="hdfs://master:9000/output"
	// path="hdfs://master:9000/output"
	public static void deleteFile(String uri, String path) {

		try {
			Configuration conf = new Configuration();

			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			fs.delete(new Path(path), true);
			System.out.println("OK!!!");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
