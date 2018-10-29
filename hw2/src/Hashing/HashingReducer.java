package lsh;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HashingReducer extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    List<String> vals = new ArrayList<String>();
    for (Text value : values) vals.add(value.toString());
    final int vSize = vals.size();
    if (vSize == 1) return;
    Collections.sort(vals);
    String[] docs = vals.toArray(new String[vSize]);
    for (int i = 0; i < vSize; ++i) {
      for (int j = i + 1; j < vSize; ++j) {
        context.write(new Text(docs[i]), new Text(docs[j]));
      }
    }
  }
}
