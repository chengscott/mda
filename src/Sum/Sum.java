package pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sum {

  private double mSum = .0;

  public Sum() {}

  public void Sum(String key, int R, String input, String output) throws Exception {
    Configuration conf = new Configuration();
    conf.set("key", key);

    Job job = Job.getInstance(conf, "Sum");
    job.setJarByClass(Sum.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setMapperClass(SumMapper.class);
    job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(DoubleWritable.class);

    job.setNumReduceTasks(R);

    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));

    job.waitForCompletion(true);

    mSum = readSumDouble(output);
  }

  public double getSum() {
    return mSum;
  }

  private double readSumDouble(String input) throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    FileStatus[] status = fs.listStatus(new Path(input));
    double ret = 0;
    for (FileStatus f : status) {
      if (!f.isFile()) continue;
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(f.getPath())));
      String res = reader.readLine();
      if (res != null) ret += Double.parseDouble(res);
      reader.close();
    }
    return ret;
  }
}
