package main;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PutPushInfo {
	public Algo algo;
	public int videoId; // the video to put
	public int videoSize; 
	public CacheServer server; // the server where we put the video
	public long gain; // the gain of score if we put the video here, regardless of it being there
	
	public List<Integer> videosOut; 
	// the set of videos to take out of server
	// that [approximately] ]minimize the loss if we take them out 
	// to make enough room for the new video
	// can be of size 0, 1, etc...
	public long sumLoss; // the sum of losses for each video taken out 
	
	private static final Logger logger = Logger.getLogger(PutPushInfo.class.getName());
	
	
	public long getAbsGain() {
		return gain-sumLoss;
	}
	
	private PutPushInfo(int videoId, CacheServer cacheServer, Algo algo) {
		this.algo = algo;
		this.videoId = videoId;
		videoSize = algo.problem.videoSizes[this.videoId];
		this.server = cacheServer;
		
		gain = 0;
		videosOut = new ArrayList<Integer>();
		sumLoss = 0;
	}
	
	private void compute(boolean allowPush) {
		
		if(videoSize > algo.problem.X) {
			logger.log(Level.WARNING, "naaaa");
		}
		
		if(server.videos.contains(videoId)) {
			logger.log(Level.WARNING, "server contains video");
		}
		
		if(!allowPush || videoSize + server.getSpaceTaken() <= algo.problem.X) {
			// gain without removing anything
			if(videoSize + server.getSpaceTaken() <= algo.problem.X) {
				gain = algo.getGainPut(server, videoId);
			} else {
				gain = -1;
			}
			
			return;
		}
		
		// we are allowed to [and will be forced to] remove some videos
		gain = algo.getGainPut(server, videoId);
		
		
		// we must take out some videos, for now we restrict to a single video
		// TODO: find good combinations of videos
		boolean foundSeveralOut = computeVideosOutBySmallestLossPerMbFreed();
		
		if(!foundSeveralOut) {
			logger.log(Level.WARNING, "no suitable take out group: cannot push");
		}
		
		
	}
	
	// the gain [may be negative] of putting video in server
	public static PutPushInfo computePutPush(int videoId, CacheServer cacheServer, Algo algo, boolean allowPush) {
		PutPushInfo ppi = new PutPushInfo(videoId, cacheServer, algo);
		ppi.compute(allowPush);
		return ppi;
	}
	
	private boolean computeVideosOutBySmallestLossPerMbFreed() {
		
		ArrayList<Video> serverVideos = new ArrayList<Video>();
		for(int v : server.videos) {
			Video video = new Video(v, algo.problem);
			video.lossOut = algo.getLossOut(server, v); 
			serverVideos.add(video);
		}
		
		Collections.sort(serverVideos, new Comparator<Video>() {
		    public int compare(Video a, Video b) {
		    	float lossPerMbA = a.lossOut*1f/a.size;
		    	float lossPerMbB = b.lossOut*1f/b.size;
		        
		    	return Float.compare(lossPerMbA, lossPerMbB);
		    	//return Integer.compare(a.lossOut, b.lossOut);
		    	//return Integer.compare(a.size, b.size);
		    }
		});
		
		// compute the loss to make room for this video
		int spaceFreed = 0;
		int totalLoss = 0;
		ArrayList<Integer> videosToTakeOut = new ArrayList<Integer>();
		for(Video video : serverVideos) {
			totalLoss += video.lossOut;
			videosToTakeOut.add(video.videoId);
			spaceFreed += video.size;
			
			if(server.getSpaceTaken() - spaceFreed + this.videoSize <= algo.problem.X){
				break;
			}
		}
		
		// also compare to the loss for each individual video
		Video bestSingleVideo = null;
		long minSingleVideoLoss = Integer.MAX_VALUE;
		for(Video video : serverVideos) {
			if(server.getSpaceTaken() - video.size + this.videoSize <= algo.problem.X) {
				if(video.lossOut < minSingleVideoLoss) {
					minSingleVideoLoss = video.lossOut;
					bestSingleVideo = video;
				}
			}
		}
		
		if(bestSingleVideo != null && minSingleVideoLoss < totalLoss) {
			// we found a good single video to take out
			this.sumLoss = minSingleVideoLoss;
			videosOut = new ArrayList<Integer>();
			videosOut.add(bestSingleVideo.videoId);
			return true;
		} else if(server.getSpaceTaken() - spaceFreed + this.videoSize <= algo.problem.X) {
			// we use the videos we found
			sumLoss = totalLoss;
			videosOut = videosToTakeOut;
			return true;
		} else {
			// no way to free enough space
			return false;
		}
		
	}
}
