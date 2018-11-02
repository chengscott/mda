package lsh;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JaccardReducer extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    String[] sigs = values.iterator().next().toString().split("^");
    String[] d0 = sigs[0].split("_"), d1 = sigs[0].split("_");
    final int K = d0.length;
    double jaccard = 0;
    for (int k = 0; k < K; ++k) if (d0[k].equals(d1[k])) ++jaccard;
    jaccard /= K;
    context.write(key, new Text(Double.toString(jaccard)));
  }
}
