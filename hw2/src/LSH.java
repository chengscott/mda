package lsh;

public class LSH {
  public static void main(String[] args) throws Exception {
    final int R = 1, K = 100;
    final String input = args[0], path = args[1];

    // Shingling
    Shingling sJob = new Shingling();
    sJob.Shingling(R, input, path + "/shingling");

    // Min Hashing
    MinHashing hJob = new MinHashing();
    hJob.MinHashing(K, R, path + "/shingling", path + "/minhashing");

    System.exit(0);
  }
}
