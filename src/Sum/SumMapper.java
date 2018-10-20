package pagerank;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SumMapper extends Mapper<Text, Text, NullWritable, DoubleWritable> {

  public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    boolean isEps = context.getConfiguration().get("key").equals("eps"),
        isKeyEps = key.toString().startsWith("eps");
    double val = Double.parseDouble(value.toString().split("\t", 2)[0]);
    if (isEps && isKeyEps) context.write(NullWritable.get(), new DoubleWritable(Math.abs(val)));
    else if (!isEps && !isKeyEps) context.write(NullWritable.get(), new DoubleWritable(val));
  }
}
