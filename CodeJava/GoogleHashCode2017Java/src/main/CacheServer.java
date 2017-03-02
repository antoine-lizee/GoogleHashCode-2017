package main;

import java.util.*;

public class CacheServer {

	public int serverId;
	
	
	public HashSet<Integer> videos; // ids of videos stored in this server
	public int spaceTaken;
	
	// returns true if actually added
	public boolean putVideo(int videoId, Algo algo, boolean ignoreSpace) {
		
		int videoSize = algo.problem.videoSizes[videoId];
		
		if(videos.contains(videoId)) {
			System.out.println("error: video already there");
			return false;
		} else if(!ignoreSpace && getSpaceTaken()+videoSize > algo.problem.X) {
			System.out.println("error: not enough space");
			return false;
		}
		
		algo.setDirty(videoId);
		
		spaceTaken += videoSize;
		videos.add(videoId);
		
		// when we put the video, some requests will use this one
		for(Request request : algo.problem.videoIdToRequests.get(videoId)) {
			EndPoint endpoint = algo.problem.endpoints.get(request.Re);
			
			if(!endpoint.latencies.containsKey(this.serverId)) {
				continue; // the request's endpoint and the cache server are not connected 
			}
			
			// latency to current server
			int currentLatency = endpoint.Ld;
			if(request.serverUsed != null) {
				currentLatency = endpoint.latencies.get(request.serverUsed.serverId);
			}
			
			int newLatency = endpoint.latencies.get(this.serverId);
			
			if(newLatency < currentLatency) {
				request.serverUsed = this;
			}
			
		}
		
		return true;
	}
	
	public boolean removeVideoNoCount(int videoId, Problem problem) {
		
		if(!videos.contains(videoId)) {
			System.out.println("error: video not there");
			return false;
		} 
		
		int videoSize = problem.videoSizes[videoId];
		videos.remove(videoId);
		spaceTaken -= videoSize;
		
		return true;
	}
	
	public ArrayList<Request> requestsUsingVideoInThisServer(int videoId, Algo algo) {
		ArrayList<Request> l = new ArrayList<Request>();
		for(Request request : algo.problem.videoIdToRequests.get(videoId)) {
			if(request.serverUsed!=null && request.serverUsed == this) {
				l.add(request);
			}	
		}
		return l;
	}
	
	public boolean removeVideoAndUpdateRequests(int videoId, Algo algo) {
		
		ArrayList<Request> affectedRequests = requestsUsingVideoInThisServer(videoId, algo);
		
		if(!this.removeVideoNoCount(videoId, algo.problem)) {
			return false;
		}
		
		algo.setDirty(videoId);
		
		for(Request request : affectedRequests) {
			request.serverUsed = Manu.findBestServerForRequest(request, algo, null);
		}
		
		return true;
		
	}
	
	
	
	public static int randomIntFromSet(Collection<Integer> myHashSet, Random rn) {
		int size = myHashSet.size();
		int item = rn.nextInt(size); 
		int i = 0;
		for(Integer aaa : myHashSet)
		{
		    if (i == item)
		        return aaa;
		    i++;
		}
		return 0;
	}
	
	public static CacheServer randomServerFromSet(Collection<CacheServer> myHashSet, Random rn) {
		int size = myHashSet.size();
		int item = rn.nextInt(size); 
		int i = 0;
		for(CacheServer aaa : myHashSet)
		{
		    if (i == item)
		        return aaa;
		    i++;
		}
		return null;
	}
	
	public CacheServer(int id) {
		serverId = id;
		videos = new HashSet<Integer>();
		spaceTaken = 0;
		//potentialVideos = new HashSet<Integer>();
	}
	
	public int getSpaceTaken() {
		return spaceTaken;
	}
	
	
}
