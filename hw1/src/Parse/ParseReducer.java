package pagerank;

import java.io.IOException;
import java.util.StringJoiner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParseReducer extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    StringJoiner adj = new StringJoiner("\t");
    adj.add("0");
    for (Text val : values) {
      String s = val.toString();
      if (!s.equals("#")) adj.add(val.toString());
    }
    context.write(key, new Text(adj.toString()));
  }
}
