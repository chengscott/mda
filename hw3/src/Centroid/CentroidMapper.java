package kmeans;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CentroidMapper extends Mapper<LongWritable, Text, Text, Text> {

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    final String[] centroid = context.getConfiguration().get("centroid").split("\t");
    final String[] val = value.toString().split(" ");
    final int D = val.length, norm = context.getConfiguration().getInt("norm", 2);
    context.getConfiguration().setInt("D", D);
    // String[] to double[]
    double[] data = new double[D];
    for (int d = 0; d < D; ++d) data[d] = Double.parseDouble(val[d]);
    // minimum cost
    int index = -1;
    double cost = 1e10;
    for (int k = 0; k < centroid.length; ++k) {
      double dis = distance(norm, data, centroid[k].split(" "));
      if (dis < cost) {
        index = k;
        cost = dis;
      }
    }
    context.write(new Text("cost"), new Text(String.valueOf(cost)));
    context.write(new Text(String.valueOf(index)), value);
  }

  private double distance(int p, final double[] data, final String[] centroid) {
    double ret = 0;
    for (int i = 0; i < data.length; ++i) {
      double d = Math.abs(data[i] - Double.parseDouble(centroid[i]));
      ret += Math.pow(d, p);
    }
    return ret;
  }
}
