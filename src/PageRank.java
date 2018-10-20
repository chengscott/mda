package pagerank;

public class PageRank {

  public static void main(String[] args) throws Exception {
    final int R = 32;
    final String input = "/user/chengscott/data/p2p-Gnutella04.txt";
    final String path = "/user/chengscott";

    Parse parseJob = new Parse();
    parseJob.Parse(R, input, path + "/parse");

    Graph graphJob = new Graph();
    graphJob.Graph(R, path + "/parse", path + "/iter/0");

    final int N = graphJob.getN();
    System.out.println(N);

    System.exit(0);
  }
}
