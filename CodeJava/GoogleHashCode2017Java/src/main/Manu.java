package main;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;



public class Manu {

	private static final Logger logger = Logger.getLogger(Manu.class.getName());
	
	public static void DoAlgo0(Algo algo, int iterations) {
		
		algo.servers.get(0).putVideo(2, algo, false);
		
		algo.servers.get(1).putVideo(1, algo, false);
		algo.servers.get(1).putVideo(3, algo, false);
		
		algo.servers.get(2).putVideo(0, algo, false);
		algo.servers.get(2).putVideo(1, algo, false);
	}
	
	public static void DoAlgo1(Algo algo, int maxOuterIter, int maxPutPerIter, int intervalBigStep, File outFile, boolean allowPush) {
		
		List<Video> initialAllocations = fillWithOptimVid(algo, maxOuterIter, maxPutPerIter, intervalBigStep, outFile, true, allowPush);
		System.out.println(String.format("Initial allocations: %d", initialAllocations.size()));
		
		//int DEPTH = 100;
		//improveAtDepth(algo, DEPTH, maxOuterIter, 1, initialAllocations);
		
    }	
	
	// take out the last "depth" allocations and try to do better
	protected static void improveAtDepth(Algo algo, int depth, int maxOuterIter, int maxPutPerIter, List<Video> allocations) {
		
		List<Video> videosToCheck = new ArrayList<Video>();
		for(int i=allocations.size()-1; i>=Math.max(0, allocations.size()-depth); i--) {
			videosToCheck.add(allocations.get(i));
		}

		int takenOut = 0;
		for(Video video : videosToCheck) {
			if(video.tmpBestServer.videos.contains(video.videoId)) {
				boolean didTakeOut = video.tmpBestServer.removeVideoAndUpdateRequests(video.videoId, algo);
				if(didTakeOut)
					takenOut++;
			} else {
				//logger.log(Level.INFO, "weird, video not in server");
			}
		}
		logger.log(Level.INFO, String.format("didTakeOut: %d/%d", takenOut, videosToCheck.size()));
		
		// compute the score if we started the last allocations with each of the removed videos
		for(Video video : videosToCheck) {
			video.scoreFinalIfPut = 0;
			boolean didPut = video.tmpBestServer.putVideo(video.videoId, algo, false);
			if(!didPut) {
				logger.log(Level.INFO, "weird, cannot put video back in server");
				continue;
			} 
			
			List<Video> tmpAllocations = fillWithOptimVid(algo, maxOuterIter, maxPutPerIter, 100, null, false, false);
			
			video.scoreFinalIfPut = algo.computeScoreFinal();
			
			// clean what we just did
			tmpAllocations.add(0, video);
			for(Video tmpVideo : tmpAllocations) {
				if(tmpVideo.tmpBestServer.videos.contains(tmpVideo.videoId)) {
					tmpVideo.tmpBestServer.removeVideoAndUpdateRequests(tmpVideo.videoId, algo);
				} else {
					logger.log(Level.INFO, "weird, tmpVideo not in server");
				}
			}
			
			video.bestNextSteps = tmpAllocations; // includes this video
			
			System.out.println(String.format("Video %d, score %d", video.videoId, video.scoreFinalIfPut));
		}
		
		// sort by best final score, do the one with the best final score
		Video chosenVideo = Collections.max(videosToCheck, new Comparator<Video>() {
			public int compare(Video a, Video b) {
				return Integer.compare(a.scoreFinalIfPut, b.scoreFinalIfPut);
		    }
		});
		
		if(chosenVideo != videosToCheck.get(0)) {
			int scoreDiff = chosenVideo.scoreFinalIfPut - videosToCheck.get(0).scoreFinalIfPut;
			System.out.println(String.format("chose different move, with score diff: %d", scoreDiff));
		}
		
		for(Video video : chosenVideo.bestNextSteps) {
			boolean didPut = video.tmpBestServer.putVideo(video.videoId, algo, false);
			if(!didPut) {
				logger.log(Level.WARNING, "weird, cannot put video in server");
			}
		}
    }
	
	// can start this from any valid state
	private static List<Video> fillWithOptimVid(Algo algo, int maxIterations, int maxPutPerIter, int intervalBigStep, File outFile, boolean verbose, boolean allowPush) {
		
		int currentScoreFinal = algo.computeScoreFinal();
		int bestScoreFinal = currentScoreFinal; 
		
		// Fill up the servers with the best videos allocations at each step and remember allocations in order
		List<Video> allocations = new ArrayList<Video>();
		
		for(int n=0; n<maxIterations; n++) {
    		
    		if(!algo.checkCorrect()) {
    			System.out.println("error");
    			return null;
    		}
    		
    		int maxPutThisIter = maxPutPerIter;
    		if(n%intervalBigStep==0) {
    			if(n>5) {
    				//maxPutThisIter = 10*maxPutPerIter;
    				maxPutThisIter = (5+rn.nextInt(50))*maxPutPerIter;
    				//cleanUp(algo);
    			}
    			
    			
    		}
    		
    		if(verbose) {
				System.out.println(n + " iterations, current score final: " + currentScoreFinal);
			}
    		
    		{
    			currentScoreFinal = algo.computeScoreFinal(); 
    			if(outFile!=null && currentScoreFinal>bestScoreFinal) {
    				bestScoreFinal = currentScoreFinal;
        			try {algo.printToFile(outFile);}
    				catch(Exception e){};
    			}
    		}
    		
			// now, for each video in the best videos to put, compute the gain after next step
    		List<Integer> videosToCompute = new ArrayList<Integer>();
    		for(int i=0; i<algo.problem.V; i++) videosToCompute.add(i);
    		
			List<Video> videos = computeStep(algo, videosToCompute, allowPush);
			
			if(videos.size()==0) {
				if(verbose) {
					System.out.println(String.format("No more gain for any video. Finished after %d iterations", n));
				}
				break;
			}
			
			{   // simply put the best videos in servers (and remove if pushed)
				
				//System.out.println(String.format("Iter %d, Max to put: %d", n, maxToPut));
				int numPut = 0;
				for(Video video : videos) {
					if(video.ppi != null) {
						// do only legal moves, so first, remove the videos legally
						for(int vOut : video.ppi.videosOut) {
							if(video.ppi.server.videos.contains(vOut)) {
								boolean takeOut = video.ppi.server.removeVideoAndUpdateRequests(vOut, algo);
								if(!takeOut) {
									logger.log(Level.INFO, "weird: video not taken out");
								}
							} // else the video was already taken out
						}
						
						
						if(!video.ppi.server.videos.contains(video.ppi.videoId) &&
								video.ppi.server.getSpaceTaken() + video.ppi.videoSize <= algo.problem.X) {

							boolean didPut = video.ppi.server.putVideo(video.ppi.videoId, algo, false);
							if(didPut) {
								numPut++;
								allocations.add(video);
							} else { 
								break;
							}
						}
					}
					if(numPut>=maxPutThisIter) {
						break;
					}
				}
				if(numPut==0) {
					System.out.println(String.format("Weird: cound not put after %d iterations", n));
					break;
				}
    		}
		}
		
		return allocations;
		
	}
	
	public static String idsToString(List<Video> videos) {
		StringBuilder sb = new StringBuilder();
		for(Video video : videos) {
			sb.append(video.videoId+", ");
		}
		sb.delete(sb.length()-2, sb.length());
		
		return sb.toString();
	}
	
	public static int NUM_THREADS = 7;
	
	// for each video, find the server [with enough capacity] where we gain most, and remember the server and associated gain
	// only keeps the videos with a gain > 0 and sorts the final list by decreasing gain 
	private static List<Video> computeStep(Algo algo, Collection<Integer> videoIdsToCompute, boolean allowPush) {
		
		
		// we will need lossOuts
		Algo.updateAllLossOutAndGainsPut(NUM_THREADS, algo, false);
		
		
		final int numTotal = algo.problem.V;
		final AtomicInteger processed = new AtomicInteger();
		ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS); 
		
		final List<Video> videos = new ArrayList<Video>();
		for(int videoId : videoIdsToCompute) {
			Video video = new Video(videoId, algo.problem);
			videos.add(video);
		}
		
		// Spawn threads
		for(final Video video : videos) {
			executor.execute(new Runnable(){
				@Override
				public void run() {
					try {
						int numProcessed = processed.incrementAndGet();
						if(allowPush) {
							//if(numProcessed%10==0) logger.info("Computing gains "+numProcessed+"/"+numTotal);
						}

						// synchronized (anything) {}
						video.tmpBestGain = Long.MIN_VALUE;
								
		    			for(CacheServer cs : algo.getPotentialServers(video.videoId)) {
		    				if(!cs.videos.contains(video.videoId)) {
		    					PutPushInfo ppi = PutPushInfo.computePutPush(video.videoId, cs, algo, allowPush);
		    					//long gain = gainPut(video.id, cs, algo.problem, false);
		    					if(ppi.getAbsGain() > video.tmpBestGain) {
		    						video.tmpBestGain = ppi.getAbsGain();
		    						video.tmpBestServer = cs;
		    						video.ppi = ppi;
		    					}
		    				}
		    			}
						
					} catch (Exception e) {
						logger.log(Level.WARNING, "Failure computing gains, videoId: " + video.videoId, e);
					}
				}
			});
		}

		executor.shutdown();
		// Wait until all threads are finished
		while (!executor.isTerminated()) {
			try {
				executor.awaitTermination(50, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
			}
		}
		//logger.log(Level.INFO, "Finished multithreaded comoutation of gains.");
		
		for(int i=videos.size()-1; i>=0; i--) {
			if(videos.get(i).ppi==null) { // happens when all servers useful for video already contain it
				videos.remove(i);
			}
		}
		
		// for faster sort, if we don't allow push, we only have positive gains
		if(!allowPush) {
			for(int i=videos.size()-1; i>=0; i--) {
				if(videos.get(i).ppi.getAbsGain() <= 0) {
					videos.remove(i);
				}
			}
		}
		
		Collections.sort(videos, new Comparator<Video>() {
		    public int compare(Video a, Video b) {
		    	//return Integer.compare(a.tmpBestGain, b.tmpBestGain);
		    	return Double.compare(a.ppi.getAbsGain()*1d/Math.pow(a.ppi.videoSize, POWER), b.ppi.getAbsGain()*1d/Math.pow(b.ppi.videoSize, POWER));
		    }
		});
		Collections.reverse(videos);
		
		return videos;
	}
	
	// gain if we put this video in this server, -1 if already there or not enough space
	public static long computeGainPut(int videoId, CacheServer cacheServer, Problem problem, boolean ignoreSpace) {
		
		if(cacheServer.videos.contains(videoId)) {
			return -1;
		}
		
		/*
		if(!cacheServer.potentialVideos.contains(videoId)) {
			return -1;
		}*/
		
		int videoSize = problem.videoSizes[videoId];
		
		if(!ignoreSpace && cacheServer.getSpaceTaken() + videoSize > problem.X) {
			return -1;
		}
		
		// when we put the video here in this server, we must look at all the requests using this video
		// and see if it changes their route, a video added can only improve scores
		long gain = 0;
		
		for(Request request : problem.videoIdToRequests.get(videoId)) {
			EndPoint endpoint = problem.endpoints.get(request.Re);
			
			if(!endpoint.latencies.containsKey(cacheServer.serverId)) {
				continue; // the request's endpoint and the cache server are not connected 
			}
			
			// latency to current server
			long currentLatency = endpoint.Ld;
			if(request.serverUsed != null) {
				currentLatency = endpoint.latencies.get(request.serverUsed.serverId);
			}
			
			long newLatency = endpoint.latencies.get(cacheServer.serverId);
			
			if(newLatency < currentLatency) {
				gain += request.Rn * (currentLatency - newLatency);
			}
			
		}
		
		return gain;
	}
	
	// The gain we would get if put each video in each server (in order)
    // If we cannot put each video in each server, returns -1 
	/*
	private static int gainPutList(List<Integer> videoIds, List<CacheServer> servers, Problem problem) {
		
		int n = videoIds.size();
		if(servers.size()!=n) {
			logger.log(Level.SEVERE, "must have as many videos as servers");
		}
		
		for(int i=0; i<n; i++) {
			if(servers.get(i).videos.contains(videoIds.get(i))) {
				return -1; // already there
			}
			
			int videoSize = problem.videoSizes[videoIds.get(i)];
			
			if(servers.get(i).getSpaceTaken() + videoSize > problem.X) {
				return -1; // not enough space
			}
		}
		
		int gain = 0;
		
		HashSet<Request> affectedRequests = new HashSet<Request>();
		for(int videoId : videoIds) {
			affectedRequests.addAll(problem.videoIdToRequests.get(videoId));
		}
		
		for(Request request : affectedRequests) {
			EndPoint endpoint = problem.endpoints.get(request.Re);
			
			// latency to current server
			int currentLatency = endpoint.Ld;
			if(request.serverUsed != null) {
				currentLatency = endpoint.latencies.get(request.serverUsed.serverId);
			}
			
			// find best latency among new connections
			int newLatency = endpoint.Ld; 
			
			for(int i=0; i<n; i++) {
				if(videoIds.get(i) != request.Rv) continue; // this video is not the request's video
				if(!endpoint.latencies.containsKey(servers.get(i).serverId)) continue; // endpoint and server not connected
				
				// here we consider that we put video i in server i, this may influence the newLatency
				int l = endpoint.latencies.get(servers.get(i).serverId);
				if(l<newLatency) {
					newLatency = l; 
				}
			}
			
			if(newLatency < currentLatency) {
				gain += request.Rn * (currentLatency - newLatency);
			}
			
		}
		
		return gain;
		
	}*/
	
	// loss of score if we take this video out of this server, -1 if video not in server
	public static long computeLossOut(int videoId, CacheServer cacheServer, Algo algo) {
			
		if(!cacheServer.videos.contains(videoId)) {
			System.out.println("Server does not have video.");
			return 0;
		}
			
		// when we take the video out of this server, 
		// we must look at all the requests using this video and server
		// see how it changes their route.
		long loss = 0;
		
		for(Request request : algo.problem.videoIdToRequests.get(videoId)) {
			if(request.Rv!=videoId || request.serverUsed != cacheServer) {
				continue;
			}
			
			long currentScore = request.computeRequestScore(algo.problem);
			
			CacheServer bestNewServer = findBestServerForRequest(request, algo, cacheServer);
			
			long newScore = 0;
			if(bestNewServer!=null) {
				EndPoint endpoint = algo.problem.endpoints.get(request.Re);
				int bestLatency = endpoint.latencies.get(bestNewServer.serverId);
				newScore = request.Rn * (endpoint.Ld - bestLatency);
			}
			
			if(newScore > currentScore) {
				System.out.println("Removing video would improve score: this should be impossible"); 
				// because each Request should always use the best possible server
			}
			
			loss += currentScore - newScore;
		}
		
		return loss;
	}
	

	
	
    // removes unused videos from the servers
    public static void cleanUp(Algo algo) {
    	
    	int cleanups = 0;
    	
    	for(CacheServer server : algo.servers) {
    		
    		ArrayList<Integer> videosToRemove = new ArrayList<Integer>();
    		
    		for(int videoId : server.videos) {
    			int numRequestsUsingVideoInServer = 0;
    			for(Request request : algo.problem.videoIdToRequests.get(videoId)) {
    				if(request.serverUsed !=null && request.serverUsed.serverId == server.serverId) {
    					numRequestsUsingVideoInServer++;
    					break;
    				}
    			}
    			
    			if(numRequestsUsingVideoInServer == 0) {
    				// no Request is using this video in this server: remove video from server
    				videosToRemove.add(videoId);
    			}
    			
    		}
    		
    		for(int v: videosToRemove) {
    			server.removeVideoAndUpdateRequests(v, algo);
    			cleanups++;
    		}
    	}
    	
    	System.out.println("cleanups: " + cleanups);
    	
    }
    
    public static void removeRandomVideos(Algo algo, int numToRemove, Random rn) {
    	
    	if(numToRemove == 0) {
    		return;
    	}
    	
    	HashSet<Request> requestsToRefind = new HashSet<Request>();
    	
    	int numVideosRemoved = 0;
    	
    	for(int i=0; i<numToRemove; i++) {
    		CacheServer cs = algo.servers.get(rn.nextInt(algo.servers.size()));
    		if(cs.videos.size()==0) continue;
    		int videoId = CacheServer.randomIntFromSet(cs.videos, rn);
    		cs.removeVideoNoCount(videoId, algo.problem);
    		numVideosRemoved++;
    		// now tell the requests using the video that its over
    		// tell the requests using this video on this server that they cannot anymore
    		
    		for(Request request : algo.problem.videoIdToRequests.get(videoId)) {
        		if(request.serverUsed == cs) {
        			request.serverUsed = null;
        			requestsToRefind.add(request);
        		}
        	}
    	}
    	
    	// find the new server they will use
    	for(Request request : requestsToRefind) {
    		request.serverUsed = findBestServerForRequest(request, algo, null);	
    	}
    	
    	System.out.println("removed: " + numVideosRemoved);
    }
    
    public static CacheServer findBestServerForRequest(Request request, Algo algo, CacheServer excluded) {
    	// find the cache servers that have the video and are connected to the endpoint
    	EndPoint endpoint = algo.problem.endpoints.get(request.Re);
    			
    	int minLatency = endpoint.Ld; // latency to datacenter
    	CacheServer bestCacheServer = null;
    			
    	for(int serverId : endpoint.latencies.keySet()) {
    				
    		CacheServer cs = algo.servers.get(serverId);
    		
    		if(excluded != null && cs.equals(excluded)) {
    			continue;
    		}
    		
    		boolean serverHasVideo = cs.videos.contains(request.Rv);
    				
    		if(serverHasVideo) {
    			int latency = endpoint.latencies.get(cs.serverId);
    			if(latency < minLatency) {
    				minLatency = latency;
    				bestCacheServer = cs;
    			}
    		}
    	}
    			
    	return bestCacheServer;
    	
    }
    
    /*
    public static double optimalPower(String nameOfFile) {
    	
    	if(nameOfFile.equals("videos_worth_spreading")) {
    		return 0.85d;
    	}
    	
    	if(nameOfFile.equals("kittens")) {
    		return 0.75d;
    	}
    	
    	if(nameOfFile.equals("me_at_the_zoo")) {
    		return 1d;
    	}
    	
    	return 1d;
    }*/
    
    private static double POWER = 1d;//0.85d; //1d
    
    public static Random rn;
    
    public static void doIt(String nameOfFile, int maxOuterIter, int maxPutPerIter, int intervalBigStep) throws IOException {
    	//Algo algo = new Algo(new Problem(new File("data/input/"+nameOfFile+".in")), nameOfFile);
    	Algo algo = Algo.readSolution(nameOfFile, 63);
	    
    	File outFile = new File("data/output/manu_"+nameOfFile+"_63.out");
	    
	    //DoAlgo1(algo, maxOuterIter, maxPutPerIter, intervalBigStep, outFile, false);
	    DoAlgo1(algo, maxOuterIter, maxPutPerIter, intervalBigStep, outFile, true);
	    algo.printToFile(outFile);
	    boolean correct = algo.checkCorrect();  
	    System.out.println(String.format("Correct: %b", correct));
	    int scoreFinal = algo.computeScoreFinal();
	    System.out.println(String.format("Score final: %d", scoreFinal));
	    
    }
    
	public static void main(String[] args) throws IOException {
		rn = new Random(23); // repeatable
		//doIt("me_at_the_zoo", Integer.MAX_VALUE, 3, 100);
		//doIt("example", Integer.MAX_VALUE, 0);
		doIt("videos_worth_spreading", Integer.MAX_VALUE, 5, 100); //(25, 200)
		//doIt("trending_today", Integer.MAX_VALUE, 100); 
		//doIt("kittens", Integer.MAX_VALUE, 20, 50);
		
	}
	
	
	
	
}
