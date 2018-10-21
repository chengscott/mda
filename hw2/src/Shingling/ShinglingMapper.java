package lsh;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ShinglingMapper extends Mapper<LongWritable, Text, Text, Text> {

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
    Text fname = new Text(filename);
    String[] val = value.toString().split(" ");
    for (int i = 0; i < val.length - 5; ++i) {
      String gram = String.join(" ", val[i], val[i + 1], val[i + 2], val[i + 3], val[i + 4]);
      context.write(fname, new Text(Integer.toString(gram.hashCode())));
    }
  }
}
