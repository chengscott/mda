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
    String val = value.toString();
    // val = val.toLowerCase();
    // val = val.replace(",", "").replace(".", "").replace("\"", "");
    String[] vals = val.split(" ");
    for (int i = 0; i < vals.length - 5; ++i) {
      String gram = String.join(" ", vals[i], vals[i + 1], vals[i + 2], vals[i + 3], vals[i + 4]);
      context.write(fname, new Text(Integer.toString(gram.hashCode())));
    }
  }
}
