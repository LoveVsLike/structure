package com.dream.hiter.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * url:http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-
 * mapreduce-client-core/MapReduceTutorial.html
 * CMD run:bin/hadoop jar wc.jar WordCount /user/joe/wordcount/input /user/joe/wordcount/output
 * @author GIT
 *
 */
public class HadoopWorldCount {

	private static class MyMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text text = new Text();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// 分割字符串
			String[] words = value.toString().split("\\s+");
			for (String word : words) {
				text.set(word);
				context.write(text, one);
			}
		}
	}

	private static class MyReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		private Text word = new Text();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values)
				sum += val.get();
			result.set(sum);
			// 自定义输出key
			word.set(key.toString());
			context.write(word, result);
		}

	}

	public static void main(String[] args) throws Exception {
		
		String dir = "hdfs://192.168.56.102:8020/user/test/hadoop/dir/start_ambari.sh";
		String dst = "hdfs://192.168.56.102:8020/user/test/hadoop/dst";
		
		// 配置信息
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "worldcount");

		job.setJarByClass(HadoopWorldCount.class);
		job.setMapperClass(MyMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(MyReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 输入、输出path
		FileInputFormat.addInputPath(job, new Path(dir));
		FileOutputFormat.setOutputPath(job, new Path(dst));

		// 结束
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
