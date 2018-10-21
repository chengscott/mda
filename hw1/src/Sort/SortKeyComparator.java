package pagerank;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortKeyComparator extends WritableComparator {

  public SortKeyComparator() {
    super(SortPair.class, true);
  }

  public int compare(WritableComparable o1, WritableComparable o2) {
    SortPair key1 = (SortPair) o1, key2 = (SortPair) o2;
    return key1.compareTo(key2);
  }
}
