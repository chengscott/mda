package lsh;

public class LSH {
  public static void main(String[] args) throws Exception {
    final int R = 1;
    final String input = args[0],
        path = args[1],
        shinglingPath = path + "/shingling",
        hashingPath = path + "/hashing",
        jaccardPath = path + "/jaccard";

    // Shingling
    Shingling shinglingJob = new Shingling();
    shinglingJob.Shingling(R, input, shinglingPath);

    // Min Hashing & LSH
    final int K = 100, r = 5;
    Hashing hashingJob = new Hashing();
    hashingJob.Hashing(K, r, R, shinglingPath, hashingPath);

    // Jaccard Similarity
    Jaccard jJob = new Jaccard();
    jJob.Jaccard(R, hashingPath, jaccardPath);

    System.exit(0);
  }
}
