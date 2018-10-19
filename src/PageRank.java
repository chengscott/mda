package pagerank;

public class PageRank {

  public static void main(String[] args) throws Exception {
    final int R = 1; // 32;
    final int iter = 1;
    final double beta = 0.8;
    // final String input = "/user/chengscott/data/p2p-Gnutella04.txt";
    final String input = "/user/chengscott/data/test.txt";
    final String path = "/user/chengscott/test";
    final String rankPath = path + "/iter";

    // Parse adjacency matrix
    Parse parseJob = new Parse();
    parseJob.Parse(R, input, path + "/parse");

    // Count N pages
    Count countJob = new Count();
    countJob.Count(path + "/parse");
    final int N = countJob.getN();

    // Add 1/N to initialize rank
    Add initJob = new Add();
    initJob.Add(R, 1. / N, path + "/parse", path + "/iter/0");

    int i;
    for (i = 1; i <= iter; ++i) {
      String prevRank = rankPath + Integer.toString(i - 1),
        curRank = rankPath + Integer.toString(i);
      // Rank
      Rank rankJob = new Rank();
      rankJob.Rank(R, beta, N, prevRank, curRank);

      // Sum new ranks

      // Add to renormalize
    }

    System.exit(0);
  }
}
