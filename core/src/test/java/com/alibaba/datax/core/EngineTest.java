package com.alibaba.datax.core;

class EngineTest {
	public static void main(String[] args) {
		System.setProperty(
			"datax.home", "/Users/h_vk/Downloads/tmp/data/datax/datax");
		String[] datxArgs = {
			"-job", "/Users/h_vk/Downloads/tmp/data/datax/test.json", "-mode", "standalone", "-jobid", "-1"
		};
		try {
			Engine.entry(datxArgs);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
