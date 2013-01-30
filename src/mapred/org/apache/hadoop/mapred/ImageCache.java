package org.apache.hadoop.mapred;

import java.awt.image.BufferedImage;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ImageCache {

	//private final static ImageCache singleton = new ImageCache();
	private static Map<String, List<BufferedImage>> map = new HashMap<String, List<BufferedImage>>();	
	//private ImageCache() {
	//	map = new HashMap<String, List<BufferedImage>>();	
	//}
	
	public static void addImage(String taskid, List<BufferedImage> bufferedImageList) {
		if(map.size() == 10) {
			map.remove(map.keySet().iterator().next());
		}
		map.put(taskid, bufferedImageList);
	}
	
	public static boolean inCache(String taskid) {
		return map.containsKey(taskid);
	}
	public static List<BufferedImage> getImageList(String taskid) {
		return map.get(taskid);
	}
	
	
	
	
}
