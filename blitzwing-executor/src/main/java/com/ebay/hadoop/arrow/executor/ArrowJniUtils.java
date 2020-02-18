package com.ebay.hadoop.arrow.executor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class ArrowJniUtils {
  private static final String LIBRARY_NAME = "arrow_executor_rs";
  private static boolean isLoaded = false;
  
  private ArrowJniUtils() {}
  
  public static void loadLibraryFromJar()
      throws IOException, IllegalAccessException {
    synchronized (ArrowJniUtils.class) {
      if (!isLoaded) {
        final String libraryToLoad = System.mapLibraryName(LIBRARY_NAME);
        final File libraryFile = moveFileFromJarToTemp(
            System.getProperty("java.io.tmpdir"), libraryToLoad);
        System.load(libraryFile.getAbsolutePath());
        isLoaded = true;
      }
    }
  }
  
  private static File moveFileFromJarToTemp(final String tmpDir, String libraryToLoad)
      throws IOException {
    final File temp = File.createTempFile(tmpDir, libraryToLoad);
    try (final InputStream is = JniExecutor.class.getClassLoader()
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
