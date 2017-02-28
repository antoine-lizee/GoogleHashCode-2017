package main;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.atomic.AtomicInteger;

public class Manu3 {
	private static final Logger logger = Logger.getLogger(Manu3.class.getName());
	
	// we start with an existing correct solution
	public static void DoAlgo3(Algo algo, File outFile, int numIterations, int requestsPerStep, boolean verbose) {
		
		for(Request rq : algo.problem.requests) {
	    	rq.computeBestPossibleGain(algo);
	    }
		
		for(int nIter=0; nIter<numIterations; nIter++) {
			
			if(nIter<5 || nIter%10==0) {
				boolean correct = algo.checkCorrect();  
			    int scoreFinal = algo.computeScoreFinal();
			    System.out.println(String.format("nIter: %d, correct: %b, score: %d", nIter, correct, scoreFinal));
				
			    if(nIter>5) {
			    	try{algo.printToFile(outFile);}
			    	catch(Exception e){System.out.println(e.getMessage());return;}
			    }
			}
			
			if(!doStep(algo, requestsPerStep, verbose)) {
				System.out.println(String.format("stopped at nIter: %d", nIter));
				break;
			}
		}
		
	}
	
	private static boolean doStep(Algo algo, final int numRequestsToLook, final boolean verbose) {
		// order requests by the difference between current gain and maximum gain (most different first)
		ArrayList<Request> orderedRequests = new ArrayList<Request>(algo.problem.requests);
		for(Request rq : algo.problem.requests) {
			rq.tmpGain = rq.computeRequestScore(algo.problem);
		}

		Collections.sort(orderedRequests, new Comparator<Request>() {
			public int compare(Request a, Request b) {
				int distA = a.bestPossibleGain - a.tmpGain;
				int distB = b.bestPossibleGain - b.tmpGain;
				if(distA<0 || distB<0) {
					System.out.println("error");
				}
				return Integer.compare(distA, distB);
			}
		});
		Collections.reverse(orderedRequests);

		
		// now, choose a request that is not being satisfied at all
		// for each server connected to the request's endpoint (and that doesn't have the video)
		//   for each video (in this server): compute the loss of gain if it is taken out
		//   for each video, compute the loss per megabyte freed, order videos by smallest loss per megabyte
		//   in this order, count the loss to free enough space for the request's video
		//     opt: if some videos taken out can be put in useful servers that have enough room for them
		//     ("freely", we should count that in the score
		//   gain for this server= gainPutVideo - lossTakeOtherVideos out
		// find the server with best gain
		// of this gain is >0, actually do the exchange.

		final ArrayList<MakeRoomInfo> goodMoves = new ArrayList<MakeRoomInfo>();
		final List<Request> requestsToLook = orderedRequests.subList(0, Math.min(orderedRequests.size(), numRequestsToLook));
		ExecutorService executor = Executors.newFixedThreadPool(7); 
		final AtomicInteger processed = new AtomicInteger();
		
		// Spawn threads
		for(final Request request : requestsToLook) {
			executor.execute(new Runnable(){
				@Override
				public void run() {
					try {
						int numProcessed = processed.incrementAndGet();
						if(false && verbose) {
							if(numProcessed%100==0) logger.info("Requests looked "+numProcessed+"/"+requestsToLook.size());
						}

						
						MakeRoomInfo bestMoveForRequest = findBestMoveForRequest(request, algo);

						if(bestMoveForRequest.absGain > 0) {

							if(verbose) {
								System.out.println(String.format("BEST idx: %d/%d gain: %d, loss: %d, numVideos: %d, absGain: %d", 
									numProcessed, numRequestsToLook,
									bestMoveForRequest.gainPut,  
									bestMoveForRequest.totalLoss, 
									bestMoveForRequest.videosToTakeOut.size(),
									bestMoveForRequest.absGain)
									);
							}
							
							synchronized (goodMoves) {
								goodMoves.add(bestMoveForRequest);
							}
						}
						
					} catch (Exception e) {
						logger.log(Level.WARNING, "Failure for request", e);
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
		
		if(verbose)
			System.out.println(String.format("found %d good moves (out of %d)", goodMoves.size(), numRequestsToLook));

		Collections.sort(goodMoves, new Comparator<MakeRoomInfo>() {
			public int compare(MakeRoomInfo a, MakeRoomInfo b) {
				return Integer.compare(a.absGain, b.absGain);
			}
		});
		Collections.reverse(goodMoves);

		if(goodMoves.size()==0)
			return false;
		
		return doMove(goodMoves.get(0), algo);
	}

	
	
	private static class MakeRoomInfo{
		int totalLoss;
		ArrayList<Integer> videosToTakeOut;
		CacheServer cs;
		int gainPut;
		int absGain;
		Request request;
		
		public MakeRoomInfo(CacheServer cs) {
			totalLoss = 0;
			videosToTakeOut = new ArrayList<Integer>();
			this.cs = cs;
		}
	}
	
	//returns true if the move was possible and made, else nothing was done
	private static boolean doMove(MakeRoomInfo move, Algo algo) {
		
		if(move.videosToTakeOut.size()!=1) {
			System.out.println(String.format("move with %d videos", move.videosToTakeOut.size()));
		}
		
		// in server cs, take out some videos, then put the request's video
		for(int videoToTakeOut : move.videosToTakeOut) {
			if(!move.cs.videos.contains(videoToTakeOut)) {
				System.out.println("video not there anymore");
				return false;
			}
		}
		
		for(int videoToTakeOut : move.videosToTakeOut) {
			boolean takenOut = move.cs.removeVideoAndUpdateRequests(videoToTakeOut, algo);
			if(!takenOut) {
				System.out.println("error");
				return false;
			}
		}
		
		boolean videoPut = move.cs.putVideo(move.request.Rv, algo.problem); 
		
		if(!videoPut) {
			System.out.println("error");
			return false;
		}
		
		// now, since we took some videos out of the server, we can try to put them somewhere else
		for(int videoToTakeOut : move.videosToTakeOut) {
			Video video = bestServerForVideo(videoToTakeOut, algo);
			if(video.tmpBestServer!=null) {
				video.tmpBestServer.putVideo(videoToTakeOut, algo.problem);
			}
		}
		
		return true;
	}
	
	public static Video bestServerForVideo(int videoId, Algo algo) {
		
		Video video = new Video(videoId, algo.problem);
		video.tmpBestGain = 0;
		
		for(CacheServer cs : algo.getPotentialServers(videoId)) {
			int gain = Manu.gainPut(videoId, cs, algo.problem, false);
			if(gain > video.tmpBestGain) {
				video.tmpBestGain = gain;
				video.tmpBestServer = cs;
			}
		}
		
		return video;
	}
	
	private static MakeRoomInfo findBestMoveForRequest(Request request, Algo algo) {
		EndPoint endpoint = algo.problem.endpoints.get(request.Re);

    	MakeRoomInfo bestMakeRoomInfo = new MakeRoomInfo(null);
    	bestMakeRoomInfo.totalLoss = Integer.MAX_VALUE;
    
    	for(Map.Entry<Integer, Integer> entry : endpoint.latencies.entrySet()) {

    		CacheServer cs = algo.servers.get(entry.getKey());
    		//int latency = entry.getValue();

    		if(cs.videos.contains(request.Rv)) {
    			continue;
    		}

    		// gain if put this video here, even though for now there is not enough space
    		int gainPut = Manu.gainPut(request.Rv, cs, algo.problem, true);

    		if(gainPut<=0) {
    			System.out.println("no gain: weird");
    			continue;
    		}
    		
    		MakeRoomInfo makeRoomInfo = computeLossToMakeRoomForVideo(algo, request.Rv, cs);
    		
    		// now, since we took some videos out of the server, we can try to put them somewhere else
    		for(int videoToTakeOut : makeRoomInfo.videosToTakeOut) {
    			Video video = bestServerForVideo(videoToTakeOut, algo);
    			if(video.tmpBestServer!=null) {
    				gainPut += video.tmpBestGain;
    			}
    		}
    		makeRoomInfo.gainPut = gainPut;
    		makeRoomInfo.absGain = makeRoomInfo.gainPut - makeRoomInfo.totalLoss;
    		makeRoomInfo.cs = cs;
    		makeRoomInfo.request = request;
    		
    		if(makeRoomInfo.absGain > bestMakeRoomInfo.absGain) {
    			bestMakeRoomInfo = makeRoomInfo;
    		}
    	}
    	
    	return bestMakeRoomInfo;
    		
	}
	
	// compute a minimal loss of request score to make room for this video in this server
	private static MakeRoomInfo computeLossToMakeRoomForVideo(Algo algo, int newVideoId, CacheServer cacheServer) {
		
		MakeRoomInfo result = new MakeRoomInfo(cacheServer);
		
		if(cacheServer.videos.contains(newVideoId)) {
			System.out.println("server already has video");
			return result;
		}
		
		int newVideoSize = algo.problem.videoSizes[newVideoId];
		
		if(cacheServer.getSpaceTaken() + newVideoSize <= algo.problem.X) {
			//System.out.println("server already has enough room for new video");
			return result;
		}
		
		ArrayList<Video> serverVideos = new ArrayList<Video>();
		for(int v : cacheServer.videos) {
			Video video = new Video(v, algo.problem);
			video.lossOut = Manu.lossOut(video.id, cacheServer, algo);
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
		for(Video video : serverVideos) {
			result.totalLoss += video.lossOut;
			result.videosToTakeOut.add(video.id);
			spaceFreed += video.size;
			
			if(cacheServer.getSpaceTaken() - spaceFreed + newVideoSize <= algo.problem.X){
				break;
			}
		}
		
		if(cacheServer.getSpaceTaken() - spaceFreed + newVideoSize > algo.problem.X){
			// even taking everything out didn't suffice
			System.out.println("video too big for server");
			return null;
		}
		
		// also try individual videos 
		boolean singleVideoOutBetter = false;
		for(Video video : serverVideos) {
			if(cacheServer.getSpaceTaken() - video.size + newVideoSize <= algo.problem.X){
				if(video.lossOut < result.totalLoss) {
					singleVideoOutBetter = true;
					result.totalLoss = video.lossOut;
					result.videosToTakeOut.clear();
					result.videosToTakeOut.add(video.id);
				}
			}
		}
		
		if(!singleVideoOutBetter) {
			//System.out.println("singleVideoOutBetter not better");
		}
		
		if(result.totalLoss<0) {
			System.out.println("totalLoss: "+result.totalLoss);
		}
		
		return result;
	}
	
	
	
	public static void doIt(String nameOfFile, int numRequestsPerIter) throws IOException {
    	
		Algo algo = Algo.readSolution(nameOfFile, 3);
		
		File outFile = new File("data/output/manu_"+nameOfFile+"_3.out");
	    
	    DoAlgo3(algo, outFile, 100, numRequestsPerIter, true);
	    algo.printToFile(outFile);
	    boolean correct = algo.checkCorrect();  
	    System.out.println(String.format("Correct: %b", correct));
	    int scoreFinal = algo.computeScoreFinal();
	    System.out.println(String.format("Score final: %d", scoreFinal));
	    
    }
	
	public static void main(String[] args) throws IOException {
		doIt("kittens", 500);
		//doIt("videos_worth_spreading", 10000);
	}
	
	
	
}
