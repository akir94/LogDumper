package org.z.logdumper.common;

import java.io.File;
import java.util.Arrays;

/**
 * Utility class converting topic + partition to file name and vice versa.
 * 
 * @author akir94
 *
 */
public class DumpFiles {
	public static File fromTopicAndPartition(String topic, int partition) {
		return new File(topic + "-" + partition + ".dump");
	}
	
	public static TopicAndPartition toTopicAndPartition(File file) {
		if (!isDumpFile(file)) {
			return null;
		} else {
			try {
				String fileName = file.getName();
				String withoutSuffix = fileName.substring(0, fileName.length()-5);
				String[] parts = withoutSuffix.split("-");
				String[] topicParts = Arrays.copyOfRange(parts, 0, parts.length-1);
				String topic = String.join("-", topicParts);
				int partition = Integer.parseInt(parts[parts.length-1]);
				return new TopicAndPartition(topic, partition);
			} catch (RuntimeException e) {
				return null;
			}
		}
	}
	
	public static boolean isDumpFile(File file) {
		String anyString = ".*";
		String hyphen = "\\-";
		String anyInteger = "\\d+";
		return file.getName().matches(anyString + hyphen + anyInteger + "\\.dump");
	}
	
	public static class TopicAndPartition {
		public String topic;
		public int partition;
		
		public TopicAndPartition(String topic, int partition) {
			this.topic = topic;
			this.partition = partition;
		}
	}
}
