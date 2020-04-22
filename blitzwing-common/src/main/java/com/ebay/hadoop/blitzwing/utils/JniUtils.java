package com.ebay.hadoop.blitzwing.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;

public class JniUtils {
  private static final Set<String> loadedLibraries = new HashSet<>();

  private JniUtils() {}

  public static void loadLibraryFromJar(String libraryName)
      throws IOException {
    synchronized (JniUtils.class) {
      if (!loadedLibraries.contains(libraryName)) {
        final String libraryToLoad = System.mapLibraryName(libraryName);
        final File libraryFile = moveFileFromJarToTemp(
            System.getProperty("java.io.tmpdir"), libraryToLoad);
        System.load(libraryFile.getAbsolutePath());
        loadedLibraries.add(libraryName);
      }
    }
  }

  private static File moveFileFromJarToTemp(final String tmpDir, String libraryToLoad)
      throws IOException {
    final File temp = File.createTempFile(tmpDir, libraryToLoad);
    try (final InputStream is = JniUtils.class.getClassLoader()
        .getResourceAsStream(libraryToLoad)) {
      if (is == null) {
        throw new FileNotFoundException(libraryToLoad);
      } else {
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    }
    return temp;
  }
}
