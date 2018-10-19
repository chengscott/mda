package pagerank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AddReducer extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    final double x = Double.parseDouble(context.getConfiguration().get("x"));
    String[] links = values.iterator().next().toString().split("\t", 2);
    double pr = Double.parseDouble(links[0]) + x;
    links[0] = Double.toString(pr);
    context.write(key, new Text(String.join("\t", links)));
  }
}
