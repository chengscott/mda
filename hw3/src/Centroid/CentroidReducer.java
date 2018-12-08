package kmeans;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CentroidReducer extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    if (key.toString().equals("cost")) {
      double cost = 0;
      for (Text value : values) cost += Double.parseDouble(value.toString());
      context.write(key, new Text(String.valueOf(cost)));
      return;
    }
    // calculate new centroid
    final int D = context.getConfiguration().getInt("D", 58);
    int count = 0;
    double[] centroid = new double[D];
    for (Text value : values) {
      String[] val = value.toString().split(" ");
      for (int d = 0; d < D; ++d) centroid[d] += Double.parseDouble(val[d]);
      ++count;
    }
    for (int d = 0; d < D; ++d) centroid[d] /= count;
    // double[] to String[]
    String[] line = new String[D];
    for (int d = 0; d < D; ++d) line[d] = String.valueOf(centroid[d]);
    context.write(new Text("centroid"), new Text(String.join(" ", line)));
  }
}
