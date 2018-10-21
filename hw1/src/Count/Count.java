package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class Count {

  private int mCount = 0;

  public Count() {}

  public void Count(String input) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Count");
    job.setJarByClass(Count.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    job.setMapperClass(CountMapper.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(0);

    FileInputFormat.addInputPath(job, new Path(input));

    job.waitForCompletion(true);

    CounterGroup counters = job.getCounters().getGroup("Nodes");
    for (Counter counter : counters) mCount += counter.getValue();
  }

  public int getN() {
    return mCount;
  }
}
