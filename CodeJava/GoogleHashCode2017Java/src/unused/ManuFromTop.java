package unused;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.*;
import java.util.Comparator;
import java.util.List;

import main.Algo;
import main.CacheServer;
import main.Manu;
import main.Request;

public class ManuFromTop {

	// fill all servers with all the potentially useful videos, going over capacity
	// as long as any server is above capacity
	// 	among the servers over capacity, remove the video with least lossOut per megabyte
	
	protected static class ServerInfo {
		CacheServer cs;
		long lossOut;
		double lossOutPerMbFreed;
		int videoId; // must be in cs.videos
		
		public ServerInfo(CacheServer cacheServer) {
			cs = cacheServer;
			lossOutPerMbFreed = Double.MAX_VALUE;
			videoId = -1;
		}
	}
	
	private static void fillUpServers(Algo algo) {
		
		for(int videoId = 0; videoId<algo.problem.V; videoId++) {
			for(CacheServer cs : algo.getPotentialServers(videoId)) {
				// put video without updating requests
				cs.spaceTaken += algo.problem.videoSizes[videoId];
				cs.videos.add(videoId);
			}
		}
		
		// now update all requests
		for(Request r : algo.problem.requests) {
			r.serverUsed = Manu.findBestServerForRequest(r, algo, null);
		}
	}
	
	// should start this from an invalid state (servers over capacity
	private static void takeOutUntilLegal(Algo algo, int maxOutPerIter, File outFile, boolean verbose) {

		int currentScoreFinal = algo.computeScoreFinal();
		
		for(int n=0; n<Integer.MAX_VALUE; n++) {

			if(algo.checkCorrect()) {
				System.out.println(String.format("Found correct solution after %d iterations.", n));
				break;
			}

			currentScoreFinal = algo.computeScoreFinal();
			
			if(n>0 && n%10==0) {
				if(verbose) {
					System.out.println(n + " iterations, current score final: " + currentScoreFinal);
				}
			}
			

			// now, for each video in the servers above capacity, compute the loss out (only)
			Algo.updateAllLossOutAndGainsPut(Manu.NUM_THREADS, algo, true);

			if(n>0 && n%10==0) {
				if(verbose) {
					System.out.println("  Done with updates ");
				}
			}
			
			if(n>0 && n%10==0) {
				File outFile2 = new File(outFile.getPath()+"_"+n);
				try {algo.printToFile(outFile2);}
				catch(Exception e){};
			}
			
			// find the smallest videos out by "loss per megabyte freed"
			List<ServerInfo> serverInfos = new ArrayList<ServerInfo>();
			
			
			for(CacheServer cs : algo.servers) {
				if(cs.getSpaceTaken() <= algo.problem.X) {
					continue; 
				}
				
				List<ServerInfo> allInfosForServer = new ArrayList<ServerInfo>();


				for(int videoId : cs.videos) {
					ServerInfo serverInfo = new ServerInfo(cs);
					serverInfo.videoId = videoId;
					serverInfo.lossOut = algo.getLossOut(cs, videoId);
					serverInfo.lossOutPerMbFreed = serverInfo.lossOut*1d/algo.problem.videoSizes[videoId];
					allInfosForServer.add(serverInfo);
				}

				int numZeroLoss = 0;

				for(ServerInfo si : allInfosForServer) {
					if(si.lossOut == 0) {
						numZeroLoss++;
						serverInfos.add(si);
					}
				}

				// add only the best
				if(numZeroLoss==0 && allInfosForServer.size()>0) {
					ServerInfo best = allInfosForServer.get(0);

					for(ServerInfo si : allInfosForServer) {
						if(si.lossOutPerMbFreed < best.lossOutPerMbFreed) {
							best = si;
						}
					}

					serverInfos.add(best);

				}


			}
			
			if(serverInfos.size() == 0) {
				System.out.println(String.format("Found no possible move at %d iterations. Breaking.", n));
				break;
			} else {
				if(verbose && n%10==0) {
					System.out.println(String.format("Moves at %d iterations: %d", n, serverInfos.size()));
				}
			}
			
			if(serverInfos.size() < maxOutPerIter) {
				Collections.sort(serverInfos, new Comparator<ServerInfo>() {
					public int compare(ServerInfo a, ServerInfo b) {
						return Double.compare(a.lossOutPerMbFreed, b.lossOutPerMbFreed);
					}
				});
			}
			
			// now take out the maxOutPerIter best
			takeAllOut(serverInfos, maxOutPerIter, algo);
		}
			
	}
	
	// returns the num actually taken out
	private static int takeAllOut(Collection<ServerInfo> serverInfos, int maxToTakeOut, Algo algo) {
		
		HashSet<Request> affectedRequests = new HashSet<Request>();
		int numTakenOut = 0;
		
		for(ServerInfo si : serverInfos) {
			
			if(si.videoId < 0) {
				System.out.println(String.format("ServerInfo move has no video."));
				break;
			}
			
			affectedRequests.addAll(si.cs.requestsUsingVideoInThisServer(si.videoId, algo));
			
			boolean takenOut = si.cs.removeVideoNoCount(si.videoId, algo.problem);
			if(!takenOut) {
				System.out.println("weird: video not taken out");
			} else {
				algo.setDirty(si.videoId);
				numTakenOut++;
			}
			
			if(numTakenOut>=maxToTakeOut) {
				break;
			}
		}
				
		for(Request request : affectedRequests) {
			request.serverUsed = Manu.findBestServerForRequest(request, algo, null);
		}
		
		return numTakenOut;
	}
	
	public static void doIt(String nameOfFile, int maxOutPerIter) throws IOException {
    	//Algo algo = new Algo(new Problem(new File("data/input/"+nameOfFile+".in")), nameOfFile);
    	//fillUpServers(algo);
		//System.out.println("servers filled");
    	
		File outFile = new File("data/output/manu_"+nameOfFile+"_200.out");
		//Algo algo = Algo.readSolution(nameOfFile, 200);
    	Algo algo = Algo.readSolution(nameOfFile, 200, outFile.getPath()+"_580");
	    
	    
    	
    	takeOutUntilLegal(algo, maxOutPerIter, outFile, true);
    	
    	algo.printToFile(outFile);
	    boolean correct = algo.checkCorrect();  
	    System.out.println(String.format("Correct: %b", correct));
	    int scoreFinal = algo.computeScoreFinal();
	    System.out.println(String.format("Score final: %d", scoreFinal));
	    
    }
    
	public static void main(String[] args) throws IOException {
		//doIt("me_at_the_zoo", 1);
		//doIt("example", Integer.MAX_VALUE, 0);
		//doIt("videos_worth_spreading", 50); 
		//doIt("trending_today", 100); 
		doIt("kittens", 10);	
	}



}

