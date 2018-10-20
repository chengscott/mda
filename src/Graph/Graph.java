package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Graph {

  private int mCount = 0;

  public Graph() {}

  public void Graph(int R, String input, String output) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Graph");
    job.setJarByClass(Graph.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setMapperClass(GraphMapper.class);
    job.setReducerClass(GraphReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(R);

    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));

    job.waitForCompletion(true);

    CounterGroup counters = job.getCounters().getGroup("Nodes");
    for (Counter counter : counters) mCount += counter.getValue();
  }

  public int getN() {
    return mCount;
  }
}
