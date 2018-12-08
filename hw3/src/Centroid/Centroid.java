package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Centroid {

  public Centroid() {}

  private String mCentroid;

  public void Centroid(
      final int R,
      final int K,
      final String centroid,
      final String distance,
      final String input,
      final String output)
      throws Exception {
    Configuration conf = new Configuration();
    conf.set("centroid", centroid);
    if (distance == "manhattan") conf.setInt("norm", 1);
    else if (distance == "euclidean") conf.setInt("norm", 2);

    Job job = Job.getInstance(conf, "Centroid");
    job.setJarByClass(Centroid.class);

    job.setMapperClass(CentroidMapper.class);
    job.setReducerClass(CentroidReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(R);

    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));

    job.waitForCompletion(true);

    readCentroid(output, K);
  }

  public String getCentroid() {
    return mCentroid;
  }

  private void readCentroid(final String input, final int K) throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    FileStatus[] status = fs.listStatus(new Path(input));
    String[] ret = new String[K];
    int k = 0;
    for (FileStatus f : status) {
      if (!f.isFile()) continue;
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(f.getPath())));
      String res;
      while ((res = reader.readLine()) != null) {
        String[] kv = res.split("\t", 2);
        if (kv[0].equals("centroid")) ret[k++] = kv[1];
      }
      reader.close();
    }
    mCentroid = String.join("\t", ret);
  }
}
