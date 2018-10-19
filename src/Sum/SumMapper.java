package pagerank;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SumMapper extends Mapper<Text, Text, NullWritable, DoubleWritable> {

  public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    double pr = Double.parseDouble(value.toString().split("\t", 2)[0]);
    context.write(NullWritable.get(), new DoubleWritable(pr));
  }
}
