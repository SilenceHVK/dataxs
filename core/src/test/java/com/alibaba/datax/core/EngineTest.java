package com.alibaba.datax.core;

class EngineTest {
  public static void main(String[] args) {
    System.setProperty(
        "datax.home", "/Users/h-vk/Documents/Project/Open-Source/DataX/DataX/target/datax/datax");
    String[] datxArgs = {
      "-job", "/Users/h-vk/Downloads/tmp/datax/job/test.json", "-mode", "standalone", "-jobid", "-1"
    };
    try {
      Engine.entry(datxArgs);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }
}
