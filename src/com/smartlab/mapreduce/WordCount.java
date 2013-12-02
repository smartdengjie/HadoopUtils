package com.smartlab.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.smartlab.hdfs.HdfsUtils;

//import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	/* ****************************
	 * 
	 * WordCount.java代码解读
	 * 
	 * @Author : dengjie
	 * 
	 * ****************************
	 * MapReduceBase类：实现了Mapper和Reducer接口到基类（其中的方法只是实现接口，而未作任何事情）
	 * 
	 * Mapper接口：
	 * 
	 * WritableComparable接口：实现WritableComparable的类可以相互比较。所有被用作key的类应该实现此接口。
	 */
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		/*
		 * LongWritable, IntWritable, Text 均是 Hadoop 中实现到用于封装 Java 数据类型的类，
		 * 这些都实现了WritableComparable接口。都能够被串行化从而便于在分布式环境中进行数据交换， 可以分别视为 long,
		 * int, String 的替代品。
		 */
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();// Text 实现类BinaryComparable类，可以作为key值

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			/*
			 * 原始数据： aaa bbb ccc bbb ccc ddd aaa bbb
			 * 
			 * map阶段，数据如下形式作为map的输入值：key为偏移量
			 * 
			 * 0 aaa
			 * 
			 * 4 bbb
			 * 
			 * 8 ccc
			 * 
			 * 12 bbb
			 * 
			 * 16 ccc
			 * 
			 * 20 ddd
			 * 
			 * 24 aaa
			 * 
			 * 28 bbb
			 * 
			 * 
			 * 以下解析后的键值对
			 * 
			 * 格式如下：前者是键，后者数字是值
			 * 
			 * aaa 1
			 * 
			 * bbb 1
			 * 
			 * ccc 1
			 * 
			 * bbb 1
			 * 
			 * ccc 1
			 * 
			 * ddd 1
			 * 
			 * aaa 1
			 * 
			 * bbb 1
			 * 
			 * 这些数据作为reduce的输出数据
			 */

			StringTokenizer itr = new StringTokenizer(value.toString());// 得到什么值
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		/*
		 * reduce过程是对输入数据解析形成如下格式数据：
		 * 
		 * (aaa [1,1])
		 * 
		 * (bbb [1,1,1])
		 * 
		 * (ccc [1,1])
		 * 
		 * (ddd [1])
		 */

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			/*
			 * 形成数据格式如下饼存储
			 * 
			 * aaa 2
			 * 
			 * bbb 3
			 * 
			 * ccc 2
			 * 
			 * ddd 1
			 */
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	private static final String uri = "hdfs://master:9000/output";
	private static final String path = "hdfs://master:9000/output";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// String[] otherArgs = new GenericOptionsParser(conf,
		// args).getRemainingArgs();
		// if (otherArgs.length != 2) {
		// System.err.println("Usage: wordcount <in> <out>");
		// System.exit(2);
		// }
		HdfsUtils.deleteFile(uri, path);
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(
				"hdfs://master:9000/namenode.log"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://master:9000/output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
