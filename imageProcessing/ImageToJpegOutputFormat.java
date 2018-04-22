package ImageToJPEGJob;

import java.io.IOException;
import java.io.OutputStream;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ImageToJpegOutputFormat extends FileOutputFormat<Object, ImageToJpegWritable> {
	TaskAttemptContext job;

	@Override
	public RecordWriter<Object, ImageToJpegWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		this.job = job;
		return new ImageToJpegRecordWriter(job);
	}

	public Path extracted(TaskAttemptContext job, String path) throws IOException {
		return getDefaultWorkFile(job, path);
	}
}

class ImageToJpegRecordWriter extends RecordWriter<Object, ImageToJpegWritable> {
	TaskAttemptContext job;
	int i = 0;
	FileSystem fs;

	ImageToJpegRecordWriter(TaskAttemptContext job) {
		this.job = job;
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException {
	}

	public String nameGenerate() {
		i++;
		return "" + i;
	}

	@Override
	public synchronized void write(Object key, ImageToJpegWritable value)
			throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		ImageToJpegOutputFormat ios = new ImageToJpegOutputFormat();
		Path file = ios.extracted(job, nameGenerate());
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, false);
		writeImage(value, fileOut);
	}

	public void writeImage(Object o, FSDataOutputStream out) throws IOException {
		if (o instanceof ImageToJpegWritable) {
			ImageToJpegWritable image = (ImageToJpegWritable) o;
			ImageIO.write(image.buffer, "jpeg", (OutputStream) out);
		}
	}

}