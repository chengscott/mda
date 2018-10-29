package lsh;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JaccardReducer extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    Map<String, Integer> docs = new HashMap<String, Integer>();
    for (Text value : values) {
      String val = value.toString();
      if (docs.containsKey(val)) docs.put(val, docs.get(val) + 1);
      else docs.put(val, 1);
    }
    for (Map.Entry<String, Integer> doc : docs.entrySet()) {
      double sim = doc.getValue() / 20;
      Text sText = new Text(Double.toString(sim));
      String out = String.format("(%s, %s):", key.toString(), doc.getKey());
      context.write(new Text(out), sText);
    }
  }
}
