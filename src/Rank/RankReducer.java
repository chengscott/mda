package pagerank;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RankReducer extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    final double beta = Double.parseDouble(conf.get("beta")),
        param = Double.parseDouble(conf.get("param"));

    String link = "";
    double sum = 0;
    for (Text value : values) {
      String val = value.toString();
      if (val.startsWith("#")) link = val.substring(1);
      else sum += Double.parseDouble(val);
    }
    // pr = beta * sum(ri / di) + param
    double pr = beta * sum + param;
    String[] links = link.split("\t", 2);
    links[0] = Double.toString(pr);
    context.write(key, new Text(String.join("\t", links)));
  }
}
