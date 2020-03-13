package com.ebay.hadoop.blitzwing.exception;

public class BlitzwingException extends RuntimeException {
  public BlitzwingException() {
  }

  public BlitzwingException(String message) {
    super(message);
  }

  public BlitzwingException(String message, Throwable cause) {
    super(message, cause);
  }

  public BlitzwingException(Throwable cause) {
    super(cause);
  }

  public BlitzwingException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
