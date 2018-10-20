package pagerank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GraphReducer extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    Text value = values.iterator().next();
    context.write(key, value);
  }
}
