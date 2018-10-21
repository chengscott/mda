package lsh;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinHashingReducer extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    //StringJoiner doc = new StringJoiner("\t");
    //for (IntWritable val : values) doc.add(val.toString());
    //context.write(key, new Text(doc.toString()));
  }
}
