package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Rank {

  public Rank() {}

  public void Rank(int R, double beta, int N, String input, String output) throws Exception {
    // param = (1 - beta) / N
    double param = (1. - beta) / N;
    Configuration conf = new Configuration();
    conf.set("beta", Double.toString(beta));
    conf.set("param", Double.toString(param));

    Job job = Job.getInstance(conf, "Rank");
    job.setJarByClass(Rank.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setMapperClass(RankMapper.class);
    job.setReducerClass(RankReducer.class);

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
