package ImageToJPEGJob;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ImageToJPEGJob extends Configured implements Tool {
	public static class ImageMapper extends Mapper<Text, ImageToJpegWritable, Text, ImageToJpegWritable> {
		@Override
		public void map(Text key, ImageToJpegWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class ImageReducer extends Reducer<Text, ImageToJpegWritable, Text, ImageToJpegWritable> {
		@Override
		public void reduce(Text key, Iterable<ImageToJpegWritable> values, Context context)
				throws IOException, InterruptedException {
			for (ImageToJpegWritable val : values) {
				context.write(key, val);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: ImageToJPEG <in> <out>");
			System.exit(2);
		}
		Job job = new Job(getConf(), "ImageToJPEG");
		job.setJarByClass(ImageToJPEGJob.class);

		job.setMapperClass(ImageMapper.class);
		job.setReducerClass(ImageReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ImageToJpegWritable.class);

		job.setInputFormatClass(ImageToJpegInputFormat.class);
		job.setOutputFormatClass(ImageToJpegOutputFormat.class);
		Configuration config=new Configuration();
		config.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
		config.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(getConf()).delete(new Path(otherArgs[1]), true);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ImageToJPEGJob(), args);
	}
}
