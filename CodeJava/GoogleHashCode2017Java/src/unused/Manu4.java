package unused;

import java.io.File;
import java.io.IOException;
import java.util.*;

import main.Algo;
import main.CacheServer;
import main.EndPoint;
import main.Problem;
import main.Request;

public class Manu4 {

	public static class Algo4Info {
		int videoId;
		int videoSize;
		
		ArrayList<CacheServer> acceptableServers;
		HashMap<CacheServer, Integer> acceptableServerGains; // gain for each individual server if we put video in it
		
		ArrayList<CacheServer> bestServerSet; // tuple of servers where we could put video
		int gain; // gain if we put videoId in ALL servers in bestServerSet
		
		public Algo4Info(int videoID, Problem problem) {
			this.videoId = videoID;
			this.videoSize = problem.videoSizes[this.videoId];
			this.gain=0;
		}
		
	}
	
	public static void DoAlgo4(Algo algo, File outFile, int outerIter) {
		
		//Random rn = new Random(23); // repeatable
    	
		for(int n=0; n<outerIter; n++) {
    		
    		if(!algo.checkCorrect()) {
    			System.out.println("error");
    			return;
    		}
    		
    		if(n<=5 || n%10==0 && n > 0) {
    			int scoreFinal = algo.computeScoreFinal();   
    			System.out.println(n + " iterations, current score final: " + scoreFinal);
    			if(n>5) {
    				try {algo.printToFile(outFile);}
    				catch(Exception e){};
    			}
    		}
    		
    		
    		// for each video, compute the gains of putting it in an optimal server, SINGLE SERVER
    		List<Algo4Info> serversInfos = new ArrayList<Algo4Info>();
    		
			for(int videoId=0; videoId<algo.problem.V; videoId++) {
    			
				Algo4Info ss_info = new Algo4Info(videoId, algo.problem);
				serversInfos.add(ss_info);
				ss_info.acceptableServers = findAcceptableServers(videoId, algo);
				
				if(ss_info.acceptableServers.size()==0) {
					//System.out.println("no acceptable server");
					continue;
				}
				
				ss_info.acceptableServerGains = new HashMap<CacheServer, Integer>();
				
    			for(CacheServer cs : ss_info.acceptableServers) {
    				ArrayList<CacheServer> l = new ArrayList<CacheServer>(); l.add(cs);
    				int gain = gainPut(videoId, l, algo, false);
    				ss_info.acceptableServerGains.put(cs,  gain);
    				
    				if(gain > ss_info.gain) { // update best just in case
    					ss_info.gain = gain;
    					ss_info.bestServerSet = l;
    				}
    			}
    			
    			boolean skipPairs = false;
    			
    			if(skipPairs) {
    				continue;
    			}
    			
    			Collections.sort(ss_info.acceptableServers, new Comparator<CacheServer>() {
    			    public int compare(CacheServer a, CacheServer b) {
    			    	return Integer.compare(
    			    			ss_info.acceptableServerGains.get(a),
    			    			ss_info.acceptableServerGains.get(b)
    			    	);
    			    }
    			});
    			Collections.reverse(ss_info.acceptableServers);
    			
    			// Now we have servers sorted by the best single-server gain
    			// take the sqrt(totalNumServers) first ones and choose the best pair of servers
    			// among the combinations, for this video
    			int maxServersToTake = (int)Math.sqrt(algo.problem.C);
    			int numToTake = Math.min(maxServersToTake, ss_info.acceptableServers.size());
    			
    			ArrayList<CacheServer> bestServers = new ArrayList<CacheServer>(ss_info.acceptableServers.subList(0, numToTake));
    			ArrayList<ArrayList<CacheServer>> pairs = getPairs(bestServers);
    			
    			for(ArrayList<CacheServer> pair : pairs) {
    				int gain = gainPut(videoId, pair, algo, false);
    				
    				if(gain > ss_info.gain) {
    					ss_info.gain = gain;
    					ss_info.bestServerSet = pair;
    				}	
    			}	
    		}
			
			// now we have all the info for each video, select the best one
			Collections.sort(serversInfos, new Comparator<Algo4Info>() {
				public int compare(Algo4Info a, Algo4Info b) {
			    	return Integer.compare(a.gain, b.gain);
			    }
			});
			Collections.reverse(serversInfos);
    		
			// put the best video in servers
			//float percentToKeep = 0.10f;//0.01f * (10-n);
			//int maxToPut = Math.max(10, (int)(algo.problem.V*percentToKeep));
			//System.out.println(String.format("Iter %d, Max to put: %d", n, maxToPut));
			int maxToPut = 1;
			int numPut = 0;
			
			for(Algo4Info serversInfo : serversInfos) {
				if(serversInfo.bestServerSet != null) {
					System.out.print(serversInfo.bestServerSet.size() + " ");
					
					gainPut(serversInfo.videoId, serversInfo.bestServerSet, algo, true);
					numPut++;
				}
				if(numPut>=maxToPut) {
					break;
				}
    		}
    		
    		if(serversInfos.get(0).gain<=0) {
    			System.out.println(String.format("Finished after %d iterations", n));
    			break;
    		}
    	}
		
		
	}
	
	
	private static ArrayList<CacheServer> findAcceptableServers(int videoId, Algo algo) {
		ArrayList<CacheServer> result = new ArrayList<CacheServer>();
		int videoSize = algo.problem.videoSizes[videoId];
		
		for(CacheServer cs : algo.getPotentialServers(videoId)) {
			if(!cs.videos.contains(videoId)) {
				if(cs.getSpaceTaken() + videoSize <= algo.problem.X) {
					result.add(cs);
				}
			}
		}
		
		return result;
	}

	// unique pairs in any order
	public static ArrayList<ArrayList<CacheServer>> getPairs(ArrayList<CacheServer> list) {
		
		ArrayList<ArrayList<CacheServer>> pairs = new ArrayList<ArrayList<CacheServer>>();
		
		for(int i=0; i<list.size(); i++) {
			for(int j=i+1; j<list.size(); j++) {
				ArrayList<CacheServer> pair = new ArrayList<CacheServer>();
				pair.add(list.get(i));
				pair.add(list.get(j));
				pairs.add(pair);
			}
		}
		
		return pairs;
	}
	
	// gain if we put this video in ALL servers of the list
	// return -1 if there is any of them where the video already is,
	// of where the video is too big, or wouldn't help
	public static int gainPut(int videoId, Collection<CacheServer> servers, Algo algo, boolean DO_PUT) {
		
		int videoSize = algo.problem.videoSizes[videoId];
		
		for(CacheServer cs : servers) {
			if(cs.videos.contains(videoId)) {
				System.out.println("video already in server");
				return -1;
			}
			/*
			if(!cs.potentialVideos.contains(videoId)) {
				System.out.println("video would never help in this server");
				return -1;
			}*/
			if(cs.getSpaceTaken() + videoSize > algo.problem.X) {
				System.out.println("not enough space for this video");
				return -1;
			}
		}
		
		// when we put the video here in these servers, we must look at all the requests using this video
		// and see if it changes their route
		int gain = 0;
		
		for(Request request : algo.problem.videoIdToRequests.get(videoId)) {
			EndPoint endpoint = algo.problem.endpoints.get(request.Re);
			
			int currentLatency = endpoint.Ld;
			if(request.serverUsed != null) {
				currentLatency = endpoint.latencies.get(request.serverUsed.serverId);
			}
			
			int bestNewLatencyForRequest = endpoint.Ld; // among new servers only (faster)
			CacheServer bestNewServerForRequest = null;
			
			for(CacheServer cs : servers) {
				
				if(!endpoint.latencies.containsKey(cs.serverId)) {
					continue; // the request's endpoint and the cache server are not connected 
				}
				
				int latencyToNewServer = endpoint.latencies.get(cs.serverId);
			
				if(latencyToNewServer < bestNewLatencyForRequest) {
					bestNewLatencyForRequest = latencyToNewServer;
					bestNewServerForRequest = cs;
					
				}
			}
			
			if(bestNewLatencyForRequest < currentLatency) {
				gain += request.Rn * (currentLatency - bestNewLatencyForRequest);
				if(DO_PUT) {
					request.serverUsed = bestNewServerForRequest;
					request.serverUsed.putVideo(videoId, algo, false);
				}
			}
		}
		
		return gain;
	}
	
	public static void doIt(String nameOfFile) throws IOException {
    	
		//Algo algo = new Algo(new Problem(new File("data/input/"+nameOfFile+".in")));
		Algo algo = Algo.readSolution(nameOfFile, 5);
		File outFile = new File("data/output/manu_"+nameOfFile+"_5.out");
	    
	    DoAlgo4(algo, outFile, 5000);
	    algo.printToFile(outFile);
	    boolean correct = algo.checkCorrect();  
	    System.out.println(String.format("Correct: %b", correct));
	    int scoreFinal = algo.computeScoreFinal();
	    System.out.println(String.format("Score final: %d", scoreFinal));
	    
    }
	
	public static void main(String[] args) throws IOException {
		//doIt("kittens");
		doIt("videos_worth_spreading");
		
	}
	
	
}
