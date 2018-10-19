package pagerank;

public class PageRank {

  public static void main(String[] args) throws Exception {
    Parse parseJob = new Parse();
    parseJob.Parse(32, "/user/chengscott/data/p2p-Gnutella04.txt", "/user/chengscott/out");

    System.exit(0);
  }
}
