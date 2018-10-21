package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sort {

  public Sort() {}

  public void Sort(String input, String output) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Sort");
    job.setJarByClass(Sort.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setMapperClass(SortMapper.class);
    job.setSortComparatorClass(SortKeyComparator.class);
    job.setGroupingComparatorClass(SortGroupComparator.class);
    job.setReducerClass(SortReducer.class);

    job.setMapOutputKeyClass(SortPair.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));

    job.waitForCompletion(true);
  }
}
