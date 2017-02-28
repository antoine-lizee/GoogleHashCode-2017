package main;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;



public class Manu {

	private static final Logger logger = Logger.getLogger(Manu.class.getName());
	
	public static void DoAlgo0(Algo algo, int iterations) {
		
		algo.servers.get(0).putVideo(2, algo.problem);
		
		algo.servers.get(1).putVideo(1, algo.problem);
		algo.servers.get(1).putVideo(3, algo.problem);
		
		algo.servers.get(2).putVideo(0, algo.problem);
		algo.servers.get(2).putVideo(1, algo.problem);
	}
	
	public static void DoAlgo1(Algo algo, int outerIter, int numVideosToRemove, File outFile) {
		
		//Random rn = new Random(23); // repeatable
    	
		boolean phaseB = false;
		
		for(int n=0; n<outerIter; n++) {
    		
    		if(!algo.checkCorrect()) {
    			System.out.println("error");
    			return;
    		}
    		
    		if(n<=5 || n%10==0 && n > 0) {
    			int scoreFinal = algo.computeScoreFinal();   
    			System.out.println(n + " iterations, current score final: " + scoreFinal);
    			//cleanUp(algo);
    			//removeRandomVideos(algo, numVideosToRemove, rn);
    			if(n>5) {
    				try {algo.printToFile(outFile);}
    				catch(Exception e){};
    			}
    			if(scoreFinal >= 0) 
    				phaseB = true;
    		}
    		
			// now, for each video in the best videos to put, compute the gain after next step
    		List<Integer> videosToCompute = new ArrayList<Integer>();
    		for(int i=0; i<algo.problem.V; i++) videosToCompute.add(i);
			List<Video> videos = computeStep(algo, videosToCompute);
			
			if(videos.size()==0) {
				System.out.println(String.format("No more gain for any video. Finished after %d iterations", n));
				break;
			}
			
			if(!phaseB){   // simply put the best videos in servers
				int maxToPut = 5; // can use 1 for all but kittens
				
				//System.out.println(String.format("Iter %d, Max to put: %d", n, maxToPut));
				int numPut = 0;
				for(Video video : videos) {
					if(video.tmpBestGain>0 && video.tmpBestServer != null && video.tmpBestServer.getSpaceTaken()+video.size<=algo.problem.X) {
						boolean didPut = video.tmpBestServer.putVideo(video.id, algo.problem);
						if(didPut)
							numPut++;
						else
							break;
					}
					if(numPut>=maxToPut) {
						break;
					}
				}
				if(numPut==0) {
					System.out.println(String.format("Cound not put after %d iterations", n));
					break;
				}
    		}
			
			else { // we are in phase B
				int DEPTH = 5;
				
				List<Video> videosToStudy = new ArrayList<Video>(videos.subList(0, Math.min(50, videos.size())));
				
				List<Integer> videosToComputeSteps = new ArrayList<Integer>();
	    		for(Video v : videos) {
	    			videosToComputeSteps.add(v.id);
	    			if(videosToComputeSteps.size() > 1000) break;
	    		}
	    		
				// for the best videos, act as if we put it in the corresponding server, 
				// and compute the gain of the next videos we would put 
				for(Video video : videosToStudy) {
					
					video.bestNextSteps = new ArrayList<Video>();
					video.tmpBestServer.putVideo(video.id, algo.problem);
					
					for(int step=0; step<DEPTH; step++) {
					
						List<Video> videosNextStep = computeStep(algo, videosToComputeSteps);
						if(videosNextStep.size()>0) {
							// put it in to compute next steps properly
							Video nextBestVideo = videosNextStep.get(0);
							nextBestVideo.tmpBestServer.putVideo(nextBestVideo.id, algo.problem);
							video.bestNextSteps.add(nextBestVideo);	
						} else {
							break;
						}
					}
					
					// clean what we just did
					video.tmpBestServer.removeVideoAndUpdateRequests(video.id, algo);
					for(Video nextStepVideo : video.bestNextSteps) {
						nextStepVideo.tmpBestServer.removeVideoAndUpdateRequests(nextStepVideo.id, algo);
					}
					
					// could also compute just sum of gains / sum of bytes taken
					video.cumulScoreNextSteps = video.tmpBestGain*1f/video.size;
					for(Video nextStepVideo : video.bestNextSteps) {
						video.cumulScoreNextSteps += nextStepVideo.tmpBestGain*1f/nextStepVideo.size;
					}
				}
				
				// select the videos which maximise the cumulative gain & space
				Collections.sort(videosToStudy, new Comparator<Video>() {
					public int compare(Video a, Video b) {
						return Float.compare(a.cumulScoreNextSteps, b.cumulScoreNextSteps);
				    }
				});
				Collections.reverse(videosToStudy);
				
				// check if the videos we chose are the same as the first videos of the initial list
				{
					HashSet<Integer> chosen = new HashSet<Integer>();
					chosen.add(videosToStudy.get(0).id);
					for(Video v : videosToStudy.get(0).bestNextSteps) chosen.add(v.id);
					
					boolean different = false;
					if(videos.size() < chosen.size()) {
						different = true;
					} else {
						for(int i=0; i<chosen.size(); i++) {
							if(!chosen.contains(videos.get(i).id)) {
								different = true;
							}
						}	
					}
					
					if(different)
						System.out.print(".");
				}
				
				//logger.log(Level.INFO, String.format("original: %d, before: %d, after: %d", videos.get(0).id, beforeID, videosToStudy.get(0).id));
			
				// finally, put the video list we found was best
    			Video best = videosToStudy.get(0);
    			boolean didPut = best.tmpBestServer.putVideo(best.id, algo.problem);
    			if(!didPut) {
    				System.out.println(String.format("Cound not put best, %d iter", n));
    			} else {
    				for(Video nextStepVideo : best.bestNextSteps) {
    				
    					boolean didPutNext = nextStepVideo.tmpBestServer.putVideo(nextStepVideo.id, algo.problem);
    					if(!didPutNext) {
    						System.out.println(String.format("Cound not put best next step, %d iter", n));
    						break;
    					}
    				}
    			}
    		}
    		
    	}
		
		
	}
	
	public static String idsToString(List<Video> videos) {
		StringBuilder sb = new StringBuilder();
		for(Video video : videos) {
			sb.append(video.id+", ");
		}
		sb.delete(sb.length()-2, sb.length());
		
		return sb.toString();
	}
	
	// for each video, find the server where we gain most, and remember the server and associated gain
	// only keeps the videos with a gain > 0 and sorts the final list by decreasing gain 
	private static List<Video> computeStep(Algo algo, Collection<Integer> videoIdsToCompute) {
		
		//final int numTotal = algo.problem.V;
		//final AtomicInteger processed = new AtomicInteger();
		ExecutorService executor = Executors.newFixedThreadPool(7); 
		
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
						//int numProcessed = processed.incrementAndGet();
						//if(numProcessed%100==0) logger.info("Computing gains "+numProcessed+"/"+numTotal);

						// synchronized (anything) {}
		    			for(CacheServer cs : algo.getPotentialServers(video.id)) {
		    				int gain = gainPut(video.id, cs, algo.problem, false);
		    				if(gain > video.tmpBestGain) {
		    					video.tmpBestGain = gain;
		    					video.tmpBestServer = cs;
		    				}
		    			}
						
					} catch (Exception e) {
						logger.log(Level.WARNING, "Failure computing gains, videoId: " + video.id, e);
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
		
		// for faster sort
		for(int i=videos.size()-1; i>=0; i--) {
			if(videos.get(i).tmpBestGain <= 0) {
				videos.remove(i);
			}
		}
		
		Collections.sort(videos, new Comparator<Video>() {
		    public int compare(Video a, Video b) {
		    	//return Integer.compare(a.tmpBestGain, b.tmpBestGain);
		    	return Float.compare(a.tmpBestGain*1f/a.size, b.tmpBestGain*1f/b.size);
		    }
		});
		Collections.reverse(videos);
		
		return videos;
	}
	
	// gain if we put this video in this server, -1 if already there or not enough space
	public static int gainPut(int videoId, CacheServer cacheServer, Problem problem, boolean ignoreSpace) {
		
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
		int gain = 0;
		
		for(Request request : problem.videoIdToRequests.get(videoId)) {
			EndPoint endpoint = problem.endpoints.get(request.Re);
			
			if(!endpoint.latencies.containsKey(cacheServer.serverId)) {
				continue; // the request's endpoint and the cache server are not connected 
			}
			
			// latency to current server
			int currentLatency = endpoint.Ld;
			if(request.serverUsed != null) {
				currentLatency = endpoint.latencies.get(request.serverUsed.serverId);
			}
			
			int newLatency = endpoint.latencies.get(cacheServer.serverId);
			
			if(newLatency < currentLatency) {
				gain += request.Rn * (currentLatency - newLatency);
			}
			
		}
		
		return gain;
	}
	
	// The gain we would get if put each video in each server (in order)
    // If we cannot put each video in each server, returns -1 
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
		
	}
	
	// loss of score if we take this video out of this server, -1 if video not in server
	public static int lossOut(int videoId, CacheServer cacheServer, Algo algo) {
			
		if(!cacheServer.videos.contains(videoId)) {
			System.out.println("Server does not have video.");
			return 0;
		}
			
		// when we take the video out of this server, 
		// we must look at all the requests using this video and server
		// see how it changes their route.
		int loss = 0;
		
		for(Request request : algo.problem.videoIdToRequests.get(videoId)) {
			if(request.Rv!=videoId || request.serverUsed != cacheServer) {
				continue;
			}
			
			int currentScore = request.computeRequestScore(algo.problem);
			
			EndPoint endpoint = algo.problem.endpoints.get(request.Re);
			//int requestVideoSize = algo.problem.videoSizes[request.Rv];
			int minLatency = endpoint.Ld; // latency to datacenter
	    	CacheServer bestCacheServer = null;
	    			
	    	for(int serverId : endpoint.latencies.keySet()) {
	    				
	    		CacheServer cs = algo.servers.get(serverId);
	    		if(cs != cacheServer) { // must be a different one
	    			boolean serverHasVideo = cs.videos.contains(request.Rv);
	    			
	    			//boolean serverHasRoomForVideo = cs.getSpaceTaken() + requestVideoSize <= algo.problem.X;
	    			
	    			if(serverHasVideo){// || serverHasRoomForVideo) {
	    				int latency = endpoint.latencies.get(cs.serverId);
	    				if(latency < minLatency) {
	    					minLatency = latency;
	    					bestCacheServer = cs;
	    				}
	    			}
	    			
	    		}
	    	}
	    	
	    	int newScore = 0;
	    	if(bestCacheServer!=null) {
	    		newScore = request.Rn * (endpoint.Ld - minLatency);
	    	}
	    	
			if(newScore > currentScore) {
				System.out.println("Removing video would improve score: there was a problem before.");
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
    				// nobody is using this video: remove video from server
    				videosToRemove.add(videoId);
    				
    			}
    			
    		}
    		
    		for(int v: videosToRemove) {
    			server.removeVideoNoCount(v, algo.problem);
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
    		request.serverUsed = findBestServerForRequest(request, algo);	
    	}
    	
    	System.out.println("removed: " + numVideosRemoved);
    }
    
    public static CacheServer findBestServerForRequest(Request request, Algo algo) {
    	// find the cache servers that have the video and are connected to the endpoint
    	EndPoint endpoint = algo.problem.endpoints.get(request.Re);
    			
    	int minLatency = endpoint.Ld; // latency to datacenter
    	CacheServer bestCacheServer = null;
    			
    	for(int serverId : endpoint.latencies.keySet()) {
    				
    		CacheServer cs = algo.servers.get(serverId);
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
    
    public static void doIt(String nameOfFile, int outerIter, int numVideosToRemove) throws IOException {
    	Algo algo = new Algo(new Problem(new File("data/input/"+nameOfFile+".in")));
	    
	    File outFile = new File("data/output/manu_"+nameOfFile+"_21.out");
	    
	    DoAlgo1(algo, outerIter, numVideosToRemove, outFile);
	    algo.printToFile(outFile);
	    boolean correct = algo.checkCorrect();  
	    System.out.println(String.format("Correct: %b", correct));
	    int scoreFinal = algo.computeScoreFinal();
	    System.out.println(String.format("Score final: %d", scoreFinal));
	    
    }
    
	public static void main(String[] args) throws IOException {
		//doIt("me_at_the_zoo", Integer.MAX_VALUE, 0);
		//doIt("example", Integer.MAX_VALUE, 0);
		doIt("videos_worth_spreading", Integer.MAX_VALUE, 0); // 10000, 1, 5
		//doIt("trending_today", Integer.MAX_VALUE, 0); // 3, 1, 0
		//doIt("kittens", Integer.MAX_VALUE, 0);
		
	}
	
	
	
	
}
