package main;

import java.util.*;

public class Video {

	public int videoId;
	public int size;
	
	public long tmpBestGain;
	public CacheServer tmpBestServer;
	public long lossOut;
	
	public List<Video> bestNextSteps;
	public int scoreFinalIfPut;
	
	public PutPushInfo ppi;
	
	public Video(int videoId, Problem problem) {
		this.videoId = videoId;
		this.size = problem.videoSizes[this.videoId];
		tmpBestGain= 0;
		tmpBestServer = null;
		lossOut = Integer.MAX_VALUE;
	}
	
}
