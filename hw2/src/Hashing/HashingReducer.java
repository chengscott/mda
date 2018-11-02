package lsh;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HashingReducer extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    List<String> vals = new ArrayList<String>();
    Map<String, String> sigs = new HashMap<String, String>();
    for (Text value : values) {
      String[] val = value.toString().split("_", 2);
      vals.add(val[0]);
      sigs.put(val[0], val[1]);
    }
    final int vSize = vals.size();
    if (vSize == 1) return;
    Collections.sort(vals);
    String[] docs = vals.toArray(new String[vSize]);
    for (int i = 0; i < vSize; ++i) {
      for (int j = i + 1; j < vSize; ++j) {
        String out = String.format("(%s, %s):", docs[i], docs[j]);
        String di = sigs.get(docs[i]), dj = sigs.get(docs[j]);
        context.write(new Text(out), new Text(di + "^" + dj));
      }
    }
  }
}
