package pagerank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ParseMapper extends Mapper<Text, Text, Text, Text> {
  public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    context.write(key, value);
    context.write(value, new Text("#"));
  }
}
