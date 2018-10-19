package pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParseReducer extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    List<String> links = new ArrayList<String>();
    /*for (Text val : values) {
        String s = val.toString();
        if (!s.equals("#"))
            links.add(val.toString());
    }
    for (String link : links) {
        if (!link.equals("#")) {
            context.write(key, new Text(link));
        }
    }
    if (titles.contains("#")) {
      for (String title : titles) {
        if (title.equals("#")) context.write(key, new Text("#"));
        else context.write(new Text(title), key);
      }
    }*/
  }
}
