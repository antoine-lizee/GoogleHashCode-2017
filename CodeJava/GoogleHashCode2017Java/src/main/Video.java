package main;

import java.util.*;

public class Video {

	public int id;
	public int size;
	
	public int tmpBestGain;
	public CacheServer tmpBestServer;
	public int lossOut;
	
	public List<Video> bestNextSteps;
	public float cumulScoreNextSteps;
	
	public Video(int videoId, Problem problem) {
		this.id = videoId;
		this.size = problem.videoSizes[this.id];
		tmpBestGain= 0;
		tmpBestServer = null;
		lossOut = 0;
	}
}
