package lsh;

import java.io.IOException;
import java.util.Random;
import java.util.StringJoiner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HashingMapper extends Mapper<Text, Text, Text, Text> {

  private int[] mA, mB;
  private int mK, mR, mBands;
  private long mP = 2147483659L;

  protected void setup(Context context) throws IOException, InterruptedException {
    mK = context.getConfiguration().getInt("K", 100);
    mR = context.getConfiguration().getInt("r", 5);
    mBands = mK / mR;
    mA = new int[mK];
    mB = new int[mK];
    Random rnd = new Random();
    for (int k = 0; k < mK; ++k) {
      mA[k] = rnd.nextInt();
      mB[k] = rnd.nextInt();
    }
  }

  public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    String[] shingling = value.toString().split("\t");
    int ngramLen = shingling.length;
    int[] ngram = new int[ngramLen];
    for (int i = 0; i < ngramLen; ++i) ngram[i] = Integer.parseInt(shingling[i]);
    // Min Hashing
    String[] sigs = new String[mK];
    for (int k = 0; k < mK; ++k) {
      int sig = Integer.MAX_VALUE, a = mA[k], b = mB[k];
      for (int i = 0; i < ngramLen; ++i) {
        int hash = universalHash(a, b, ngram[i]);
        if (hash < sig) sig = hash;
      }
      sigs[k] = Integer.toString(sig);
    }
    // LSH
    String signature = key.toString() + "_" + String.join("_", sigs);
    Text val = new Text(signature);
    for (int b = 0; b < mBands; ++b) {
      StringJoiner hash = new StringJoiner("_");
      hash.add(Integer.toString(b));
      for (int r = 0; r < mR; ++r) hash.add(sigs[b * mR + r]);
      int lsh = hash.toString().hashCode();
      context.write(new Text(Integer.toString(lsh)), val);
    }
  }

  private int universalHash(int a, int b, int x) {
    long res = (((long) a) * ((long) x)) % mP;
    res = (res + b) % mP;
    return (int) (res % Integer.MAX_VALUE);
  }
}
