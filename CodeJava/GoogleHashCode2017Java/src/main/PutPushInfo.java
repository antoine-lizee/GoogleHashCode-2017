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
		
		// option 1: sort videos in increasing loss per byte, then take by smallest loss per byte
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
		int lpmf_spaceFreed = 0;
		int lpmf_totalLoss = 0;
		ArrayList<Integer> lpmf_videos = new ArrayList<Integer>();
		for(Video video : serverVideos) {
			lpmf_totalLoss += video.lossOut;
			lpmf_videos.add(video.videoId);
			lpmf_spaceFreed += video.size;
			
			if(server.getSpaceTaken() - lpmf_spaceFreed + this.videoSize <= algo.problem.X){
				break;
			}
		}
		
		// options 2: sort videos in increasing loss, then take by smallest loss
		Collections.sort(serverVideos, new Comparator<Video>() {
		    public int compare(Video a, Video b) {
		    	return Long.compare(a.lossOut, b.lossOut);
		    }
		});
		
		// compute the loss to make room for this video
		int lo_spaceFreed = 0;
		int lo_totalLoss = 0;
		ArrayList<Integer> lo_videos = new ArrayList<Integer>();
		for(Video video : serverVideos) {
			lo_totalLoss += video.lossOut;
			lo_videos.add(video.videoId);
			lo_spaceFreed += video.size;
			
			if(server.getSpaceTaken() - lo_spaceFreed + this.videoSize <= algo.problem.X){
				break;
			}
		}
		
		
		// option 3:  also compare to the loss for each individual video, and pairs of small videos 
		Video bestSingleVideo = null;
		long minSingleVideoLoss = Integer.MAX_VALUE;
		List<Video> smallVideos = new ArrayList<Video>(); // videos that are not enough alone
		for(Video video : serverVideos) {
			if(server.getSpaceTaken() - video.size + this.videoSize <= algo.problem.X) {
				if(video.lossOut < minSingleVideoLoss) {
					minSingleVideoLoss = video.lossOut;
					bestSingleVideo = video;
				}
			} else {
				smallVideos.add(video);
			}
		}
		
		Video bestPairedVideo_i = null;
		Video bestPairedVideo_j = null;
		long minPairedVideosLoss = Integer.MAX_VALUE;
		
		
		for(int i=0; i<smallVideos.size(); i++) {
			Video vi = smallVideos.get(i);
			for(int j=i+1; j<smallVideos.size(); j++) {
				Video vj = smallVideos.get(j);
				if(server.getSpaceTaken() - vi.size - vj.size + this.videoSize <= algo.problem.X) {
					long pairedLoss = vi.lossOut + vj.lossOut;
					if(pairedLoss < minPairedVideosLoss) {
						minPairedVideosLoss = pairedLoss;
						bestPairedVideo_i = vi;
						bestPairedVideo_j = vj;
					}
				}
			}
		}
		
		boolean singleIsBest = bestSingleVideo != null 
				&& minSingleVideoLoss <= lpmf_totalLoss 
				&& minSingleVideoLoss <= lo_totalLoss
				&& minSingleVideoLoss <= minPairedVideosLoss;
		
		boolean pairIsBest = bestPairedVideo_i != null && bestPairedVideo_j != null 
				&& minPairedVideosLoss <= lpmf_totalLoss 
				&& minPairedVideosLoss <= lo_totalLoss
				&& minPairedVideosLoss <= minSingleVideoLoss;
				
		
		if(singleIsBest) {
			// we found a good single video to take out
			this.sumLoss = minSingleVideoLoss;
			videosOut = new ArrayList<Integer>();
			videosOut.add(bestSingleVideo.videoId);
			return true;
		} else if(pairIsBest) {
			this.sumLoss = minPairedVideosLoss;
			videosOut = new ArrayList<Integer>();
			videosOut.add(bestPairedVideo_i.videoId);
			videosOut.add(bestPairedVideo_j.videoId);
			return true;
		} else {
			if(lpmf_totalLoss <= lo_totalLoss) {
				if(server.getSpaceTaken() - lpmf_spaceFreed + this.videoSize <= algo.problem.X) {
					sumLoss = lpmf_totalLoss;
					videosOut = lpmf_videos;
					return true;
				} else {
					// no way to free enough space
					return false;
				}
			} else {
				if(server.getSpaceTaken() - lo_spaceFreed + this.videoSize <= algo.problem.X) {
					sumLoss = lo_totalLoss;
					videosOut = lo_videos;
					return true;
				} else {
					// no way to free enough space
					return false;
				}
			}
		} 
		
	}
}
