package lsh;

public class LSH {
  public static void main(String[] args) throws Exception {
    final int R = 1;
    final String input = args[0], path = args[1];

    // Shingling
    // Shingling sJob = new Shingling();
    // sJob.Shingling(R, input, path + "/shingling");

    // Min Hashing & LSH
    final int K = 100, r = 5;
    Hashing hashJob = new Hashing();
    hashJob.Hashing(K, r, R, path + "/shingling", path + "/hashing");

    //Jaccard jJob = new Jaccard();
    //jJob.Jaccard(R, path + "/hashing", path + "/jaccard");

    System.exit(0);
  }
}
