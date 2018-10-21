package lsh;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Shingling {

  public Shingling() {}

  public void Shingling(int R, String input, String output) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Shingling");
    job.setJarByClass(Shingling.class);

    job.setMapperClass(ShinglingMapper.class);
    job.setReducerClass(ShinglingReducer.class);

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
