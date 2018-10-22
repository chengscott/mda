package lsh;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Hashing {

  public Hashing() {}

  public void Hashing(int K, int r, int R, String input, String output) throws Exception {
    Configuration conf = new Configuration();
    conf.setInt("K", K);
    conf.setInt("r", r);

    Job job = Job.getInstance(conf, "Hashing");
    job.setJarByClass(Hashing.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setMapperClass(HashingMapper.class);
    job.setReducerClass(HashingReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(R);

    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));

    job.waitForCompletion(true);
  }
}
