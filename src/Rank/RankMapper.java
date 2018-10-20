package pagerank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankMapper extends Mapper<Text, Text, Text, Text> {
  public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    if (key.toString().startsWith("eps")) return;
    String[] links = value.toString().split("\t");
    int deg = links.length - 1;
    if (deg > 0) {
      double pr = Double.parseDouble(links[0]) / deg;
      Text p = new Text(Double.toString(pr));
      for (int i = 1; i <= deg; ++i) context.write(new Text(links[i]), p);
    }
    context.write(key, new Text("#" + value.toString()));
  }
}
