package lsh;

import java.io.IOException;
import java.util.Random;
import java.util.StringJoiner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinHashingMapper extends Mapper<Text, Text, Text, Text> {

  private int[] mA, mB;
  private int mK;
  private long mP = 2147483647;

  protected void setup(Context context) throws IOException, InterruptedException {
      mK = Integer.parseInt(context.getConfiguration().get("K"));
      mA = new int[mK];
      mB = new int[mK];
      Random rnd = new Random();
      for (int k = 0; k < mK; ++k) {
        mA[k] = rnd.nextInt();
        mB[k] = rnd.nextInt();
      }
  }

  public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
      String[] shingling = value.toString().split("\t");
      int ngramLen = shingling.length;
      int[] ngram = new int[ngramLen];
      for (int i = 0; i < ngramLen; ++i)
        ngram[i] = Integer.parseInt(shingling[i]);
      //int[] signature = new int[K];
      StringJoiner signature = new StringJoiner("\t");
      for (int k = 0; k < mK; ++k) {
        int sig = Integer.MAX_VALUE, a = mA[k], b = mB[k];
        for (int i = 0; i < ngramLen; ++i) {
            int hash = universalHash(a, b, ngram[i]);
            if (hash < sig) sig = hash;
        }
        //signature[k] = sig;
        signature.add(Integer.toString(sig));
      }
      context.write(key, new Text(signature.toString()));
  }

  private int universalHash(int a, int b, int x) {
      long res = (((long) a) * ((long) x)) % mP;
      res = (res + b) % mP;
      return (int) (res % Integer.MAX_VALUE);
  }
}
