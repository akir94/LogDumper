package org.z.logdumper.common;

import java.util.Arrays;

/**
 * Utility class converting topic + partition to file name and vice versa.
 * 
 * @author akir94
 *
 */
public class DumpFiles {
	public static String nameFromTopicAndPartition(String topic, int partition) {
		return topic + "-" + partition + ".dump";
	}
	
	/**
	 * Given a dump file, returns the topic and partition this file corresponds to,
	 * or null if it's not a dump file.
	 * 
	 * @param file
	 * @return topic and partition, or null if not a dump file
	 */
	public static TopicAndPartition topicAndPartitionFromName(String fileName) {
		if (!isDumpFile(fileName)) {
			return null;
		} else {
			try {
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
	
	public static boolean isDumpFile(String fileName) {
		String anyString = ".*";
		String hyphen = "\\-";
		String anyInteger = "\\d+";
		return fileName.matches(anyString + hyphen + anyInteger + "\\.dump");
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
