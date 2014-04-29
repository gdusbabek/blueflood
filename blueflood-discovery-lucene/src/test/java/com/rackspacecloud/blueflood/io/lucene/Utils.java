package com.rackspacecloud.blueflood.io.lucene;

import java.io.File;
import java.math.BigInteger;
import java.util.Random;

public class Utils {
    private static final Random random = new Random(System.nanoTime());
        
    public static File getRandomTempDir() {
        String path = System.getProperty("java.io.tmpdir");
        String rand = "__blueflood_lucene_test_" + new BigInteger(128, random).toString(16);
        File f = new File(new File(path), rand);
        if (!f.mkdirs())
            throw new RuntimeException("Could not make temp dir");
        return f;
    }
    
    public static void removeDir(File f) {
        if (f.isDirectory()) {
            for (File ch : f.listFiles()) {
                removeDir(ch);
            }
        }
        f.delete();
    }
}
