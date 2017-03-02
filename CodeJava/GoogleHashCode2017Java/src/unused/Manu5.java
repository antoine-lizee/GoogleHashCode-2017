package unused;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import main.Algo;
import main.CacheServer;
import main.Manu;
import main.Video;



public class Manu5 {

	// take/make a solution to the same problem but with a higher server capacity
	// then try to modify it until we have a solutiuon to our current problem
	
	private static final Logger logger = Logger.getLogger(Manu5.class.getName());
	
	public static void DoAlgo(Algo algo, int outerIter, File outFile) {
		
		//Random rn = new Random(23); // repeatable
    	
		int currentScoreFinal = 0;
		
		for(int n=0; n<outerIter; n++) {
    		
    		if(algo.checkCorrect()) {
    			System.out.println(String.format("Algo is now correct. Finished after %d iterations", n));
    			break;
    		}
    		
    		currentScoreFinal = algo.computeScoreFinal();   
    		
    		if(n<=5 || n%10==0 && n > 0) {
    			System.out.println(n + " iterations, current score final: " + currentScoreFinal);
    			if(n>5) {
    				try {algo.printToFile(outFile);}
    				catch(Exception e){};
    			}
    		}
    		
			/*
			List<Video> videos = computeLossesStepVideo(algo);
			if(videos.size()==0) {
				System.out.println(String.format("No more gain for any video. Finished after %d iterations", n));
				break;
			}*/
    		
    		List<ServerInfo> serverInfos = computeLossesStepServer(algo);
			if(serverInfos.size()==0) {
				System.out.println(String.format("No more gain for any server. Finished after %d iterations", n));
				break;
			}
			
			
			{   // simply take out the videos (with smallest loss per mb) from servers
				
				int maxToTakeOut = 10; // can use 1 for all but kittens
				
				int numTakenOut = 0;
				for(ServerInfo si : serverInfos) {
					if(si.server.videos.contains(si.tmpBestVideoId) ) {
						boolean didTakeOut = si.server.removeVideoAndUpdateRequests(si.tmpBestVideoId, algo);	
						if(didTakeOut) {
							System.out.println("Taken out from server: " + si.server.serverId);
							
							numTakenOut++;
							
							// now, since we took some videos out of the server, we can try to put them somewhere else
							/*
							Video v2 = Manu3.bestServerForVideo(si.tmpBestVideoId, algo);
							if(v2.tmpBestServer!=null) {
								v2.tmpBestServer.putVideo(v2.id, algo.problem, false);
							}*/
						} else {
							System.out.println("Could not take out from server: " + si.server.serverId);
							break;
						}
					} else {
						System.out.println(String.format("Server %d does not have video %d", si.server.serverId, si.tmpBestVideoId));
					}
					if(numTakenOut>=maxToTakeOut) {
						break;
					}
				}
				
				if(numTakenOut==0) {
					System.out.println(String.format("Cound not take out after %d iterations, with %d si", n, serverInfos.size()));
					break;
				}
    		}	
    	}	
	}
	
	public static class ServerInfo {
		CacheServer server;
		
		int tmpBestVideoId;
		long tmpSmallestLoss;
		double tmpSmallestLossPerMegabyteFreed; 
		
		public ServerInfo(CacheServer cs) {
			server = cs;
			tmpBestVideoId = -1;
			tmpSmallestLoss = Integer.MAX_VALUE;
			tmpSmallestLossPerMegabyteFreed = Float.MAX_VALUE;
		}
		
	}
	
	// for each server over capacity, find the video with the smallest loss per megabyte freed
	// (if we take this video out)
	// and remember the video and associated loss
	// there can be a  loss == 0, and this sorts the final list by increasing loss per megabyte freed 
	protected static List<ServerInfo> computeLossesStepServer(Algo algo) {
		
		//final int numTotal = algo.problem.V;
		//final AtomicInteger processed = new AtomicInteger();
		ExecutorService executor = Executors.newFixedThreadPool(Manu.NUM_THREADS); 
		
		final List<ServerInfo> serverInfos = new ArrayList<ServerInfo>();
		for(CacheServer cs : algo.servers) {
			if(cs.getSpaceTaken() > algo.problem.X) {
				ServerInfo si = new ServerInfo(cs);
				serverInfos.add(si);
			}
		}
		
		
		// Spawn threads
		for(final ServerInfo serverInfo : serverInfos) {
			executor.execute(new Runnable(){
				@Override
				public void run() {
					try {
						//int numProcessed = processed.incrementAndGet();
						//if(numProcessed%100==0) logger.info("Computing gains "+numProcessed+"/"+numTotal);

						// synchronized (anything) {}
						
		    			for(int videoId : serverInfo.server.videos) {
		    				
		    				long loss = Manu.computeLossOut(videoId, serverInfo.server, algo);
		    				
		    				float lossPerMegabyteFreed = loss*1f/algo.problem.videoSizes[videoId]; 
		    				
		    				
		    				if(lossPerMegabyteFreed < serverInfo.tmpSmallestLossPerMegabyteFreed) {
		    					serverInfo.tmpBestVideoId = videoId;
		    					serverInfo.tmpSmallestLossPerMegabyteFreed = lossPerMegabyteFreed;
		    					serverInfo.tmpSmallestLoss = loss;
		    				}
		    			}
						
					} catch (Exception e) {
						logger.log(Level.WARNING, "Failure computing losses, serverId: " + serverInfo.server.serverId, e);
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
		
		Collections.sort(serverInfos, new Comparator<ServerInfo>() {
		    public int compare(ServerInfo a, ServerInfo b) {
		    	return Long.compare(a.tmpSmallestLoss, b.tmpSmallestLoss);
		    	//return Double.compare(a.tmpSmallestLossPerMegabyteFreed, b.tmpSmallestLossPerMegabyteFreed);
		    }
		});
		
		return serverInfos;
	}
	
	// for each video, find the server [over capacity and containing cideo]
	// the smallest loss (per megabyte freed) if we took this video out
	protected static List<Video> computeLossesStepVideo(Algo algo) {
		
		//final int numTotal = algo.problem.V;
		//final AtomicInteger processed = new AtomicInteger();
		ExecutorService executor = Executors.newFixedThreadPool(Manu.NUM_THREADS); 
		
		final List<Video> videos = new ArrayList<Video>();
		for(int videoId=0; videoId<algo.problem.V; videoId++) {
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
						
		    			for(CacheServer cs : algo.getPotentialServers(video.videoId)) {
		    				if(!cs.videos.contains(video.videoId)) {
		    					continue; 
		    				}
		    				if(cs.getSpaceTaken() <= algo.problem.X) {
		    					continue;
		    				}
		    				
		    				long loss = Manu.computeLossOut(video.videoId, cs, algo);
		    				
		    				if(loss < video.lossOut) {
		    					video.lossOut = loss;
		    					video.tmpBestServer = cs;
		    				}
		    			}
						
					} catch (Exception e) {
						logger.log(Level.WARNING, "Failure computing losses, videoId: " + video.videoId, e);
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
		
		Collections.sort(videos, new Comparator<Video>() {
		    public int compare(Video a, Video b) {
		    	return Long.compare(a.lossOut, b.lossOut);
		    	//return Double.compare(a.lossOut*1d/a.size, b.lossOut*1d/b.size);
		    }
		});
		
		return videos;
	}
	
    public static void doIt(String nameOfFile, int outerIter, int numVideosToRemove) throws IOException {
    	
    	//Algo algo = new Algo(new Problem(new File("data/input/"+nameOfFile+".in")));
	    
    	 // read a solution to the pb with capacity = 1.01 * capacity
    	Algo algo = Algo.readSolution(nameOfFile, 41);
	    File outFile = new File("data/output/manu_"+nameOfFile+"_42.out");
	    
	    DoAlgo(algo, outerIter, outFile);
	    algo.printToFile(outFile);
	    boolean correct = algo.checkCorrect();  
	    System.out.println(String.format("Correct: %b", correct));
	    int scoreFinal = algo.computeScoreFinal();
	    System.out.println(String.format("Score final: %d", scoreFinal));
	    
    }
    
	public static void main(String[] args) throws IOException {
		//doIt("me_at_the_zoo", Integer.MAX_VALUE, 0);
		//doIt("example", Integer.MAX_VALUE, 0);
		//doIt("videos_worth_spreading", Integer.MAX_VALUE, 0); // 10000, 1, 5
		//doIt("trending_today", Integer.MAX_VALUE, 0); // 3, 1, 0
		doIt("kittens", Integer.MAX_VALUE, 0);
		
	}
	
	
	
	
}
