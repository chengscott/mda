package kmeans;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class KMeans {
  public static void main(String[] args) throws Exception {
    final int R = 1, iter = 20, K = 10;
    final String input = args[0], centroidFile = args[1], distance = args[2], outputPath = args[3];

    // initialize centroid
    FileSystem fs = FileSystem.get(new Configuration());
    BufferedReader bufferedReader =
        new BufferedReader(new InputStreamReader(fs.open(new Path(centroidFile))));
    String[] lines = new String[K];
    for (int i = 0; i < K; ++i) lines[i] = bufferedReader.readLine();
    String centroid = String.join("\t", lines);
    // iterations
    for (int i = 1; i <= iter; ++i) {
      String output = outputPath + "/" + Integer.toString(i);

      Centroid centroidJob = new Centroid();
      centroidJob.Centroid(R, K, centroid, distance, input, output);
      centroid = centroidJob.getCentroid();
    }

    System.exit(0);
  }
}
