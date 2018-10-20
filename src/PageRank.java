package pagerank;

public class PageRank {

  public static void main(String[] args) throws Exception {
    final int R = 1, iter = -1;
    final double beta = 0.8;
    final String input = "/user/chengscott/data/p2p-Gnutella04.txt";
    // final String input = "/user/chengscott/data/test.txt";
    final String path = "/user/chengscott/p2p-20";
    final String rankPath = path + "/iter/",
        tmpPath = path + "/tmp/",
        sumPath = path + "/sum/",
        convPath = path + "/conv/";

    // Parse adjacency matrix
    Parse parseJob = new Parse();
    parseJob.Parse(R, input, path + "/parse");

    // Count N pages
    Count countJob = new Count();
    countJob.Count(path + "/parse");
    final int N = countJob.getN();

    // Add 1 / N to initialize rank
    Add initJob = new Add();
    initJob.Add(R, 1. / N, path + "/parse", rankPath + "0");

    double eps = N;
    int i;
    for (i = 1; iter == -1 ? !(eps < 1e-15) : i <= iter; ++i) {
      String prevRank = rankPath + Integer.toString(i - 1),
          tmpRank = tmpPath + Integer.toString(i),
          curRank = rankPath + Integer.toString(i),
          curSum = sumPath + Integer.toString(i),
          curConv = convPath + Integer.toString(i);
      // Rank
      Rank rankJob = new Rank();
      rankJob.Rank(R, beta, N, prevRank, tmpRank);

      // Sum rank
      Sum sumJob = new Sum();
      sumJob.Sum("rank", R, tmpRank, curSum);
      final double S = sumJob.getSum();

      // Add to renormalize
      Add normJob = new Add();
      normJob.Add(R, (1. - S) / N, tmpRank, curRank);

      // Sum epsilon
      if (iter == -1) {
        Sum convJob = new Sum();
        convJob.Sum("eps", R, curRank, curConv);
        eps = convJob.getSum();
      }
    }
    // Sort Result
    Sort sortJob = new Sort();
    sortJob.Sort(rankPath + Integer.toString(i - 1), path + "/sort");

    System.exit(0);
  }
}
