package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Algo {
	
	String dataName;
	public Problem problem;
	
	public ArrayList<CacheServer> servers;
	//HashMap<Integer, HashSet<CacheServer>> videoIdToPotentialServers;
	
	private static final Logger logger = Logger.getLogger(Algo.class.getName());
	
	public void printToFile(File file) throws IOException {
		FileOutputStream fos = new FileOutputStream(file);
	 	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
	 
		bw.write(Integer.toString(servers.size()));
		bw.newLine();
		for(CacheServer server : servers) {
			StringBuilder sb =new StringBuilder();
			sb.append(server.serverId);
			
			for(Integer videoId : server.videos) {
				sb.append(" "+videoId);
			}
			
			
			bw.write(sb.toString());
			bw.newLine();
		}
	 	bw.close();
	 	
	 	System.out.println(String.format("Saved at: %s", file.getPath()));
	}
	
	public void readFromFile(File file) throws IOException {
		FileInputStream fstream = new FileInputStream(file);
	 	BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
	 
	 	int numLine = -1;
	 	String strLine;
	 	
	 	while ((strLine = br.readLine()) != null)   {
	 	
	 		if(numLine==-1) {
	 			int readNumServers = Integer.parseInt(strLine);
	 			if(readNumServers != this.problem.C) {
	 				System.out.println(String.format("ERROR, expected {0} servers but read {1}", this.problem.C, readNumServers));
	 			}
	 		} else {
	 			String[] ss = strLine.split(" ");
	 			
	 			int serverId = Integer.parseInt(ss[0]);
	 			for(int i=1; i<ss.length; i++) {
	 				int videoId = Integer.parseInt(ss[i]);
	 				this.servers.get(serverId).putVideo(videoId, this, true);
	 			}
	 			
	 		}
			numLine++;
		}
	 	br.close();
		
	 	
	 	
	 }
	
	public boolean checkCorrect() {
		
		for(CacheServer cs : servers) {
			
			int sizeInServer = cs.getSpaceTaken();
			
			if(sizeInServer > problem.X) {
				//System.out.println("Too much at server: " + cs.serverId);
				return false;
			}
			
		}
		
		return true;
	}
	
	
	public long computeScore() {
		long n=0;
		for(Request request : problem.requests) {
			n += request.computeRequestScore(problem);
		}
		
		if(n<0) {
			System.out.println("score below 0: " +n);
		}
		
		return n;
	}
	
	public int computeScoreFinal() {
		
		double score = computeScore();
		
		int numRequests = 0;
		for(Request request : problem.requests) {
			numRequests += request.Rn;
		}
		
		double d = score / numRequests;
		int result = (int) (d*1000);
		return result;
	}
	
	// as if all servers had infinite capacity
	public int computeUpperBound() {
		
		long sumUpperBounds = 0;
		long numRequests = 0;
		for(Request request : problem.requests) {
			request.computeBestPossibleGain(this);
			sumUpperBounds += request.bestPossibleGain;
			numRequests += request.Rn;
		}
		
		double d = sumUpperBounds * 1d / numRequests;
		int result = (int) (d*1000);
		return result;
	}
	
	// as if all servers were in one server (and each endpoint's latency was its minimum latency)
	public int computeUpperBound2() {
		
		for(Request request : problem.requests) {
			request.computeBestPossibleGain(this);
		}
		
		ArrayList<Request> orderedRequests = new ArrayList<Request>(problem.requests);
		Collections.sort(orderedRequests, new Comparator<Request>() {
		    public int compare(Request a, Request b) {
		    	return Float.compare(
		    		a.bestPossibleGain*1f/problem.videoSizes[a.Rv],
		    		b.bestPossibleGain*1f/problem.videoSizes[b.Rv]
		    	);
		    }
		});
	    Collections.reverse(orderedRequests);
		
	    int totalCapacity = problem.C * problem.X;
		int totalSpaceTaken = 0;
	    
		long sumUpperBounds = 0;
		long numRequests = 0;
		HashSet<Integer> videosPut = new HashSet<Integer>();
		
		for(Request request : orderedRequests) {
			int videoSize = problem.videoSizes[request.Rv];
			if(!videosPut.contains(request.Rv) && totalSpaceTaken + videoSize <= totalCapacity) {
				// we put it
				videosPut.add(request.Rv);
				totalSpaceTaken += videoSize;
				
			}
			if(videosPut.contains(request.Rv)) {
				sumUpperBounds += request.bestPossibleGain;
			}
			numRequests += request.Rn;
		}
	    
		double d = sumUpperBounds * 1d / numRequests;
		int result = (int) (d*1000);
		return result;
	}
	
	public String getCurretInfo() {
		StringBuilder sb = new StringBuilder();
		
		
		sb.append(String.format("Data set name: %s", dataName));
		sb.append("\n");
		sb.append(String.format("V=%d, E=%d, R=%d, C=%d, X=%d", problem.V, problem.E, problem.R, problem.C, problem.X));
		sb.append("\n");
		
		{
			int totalCapacity = problem.X * problem.C;
			int totalSpaceTaken = 0;
			for(CacheServer cs : servers) totalSpaceTaken += cs.getSpaceTaken();

			sb.append(String.format("Total space taken: %d / %d", totalSpaceTaken, totalCapacity));
			sb.append("\n");
		}
		{
			int score = computeScoreFinal();
			int upperBound2 = computeUpperBound2();

			sb.append(String.format("Score final: %d, Upper bound 2: %d", score, upperBound2));
			sb.append("\n");
		}
		int[] connectionsPerServer = new int[problem.C];
		{
			int endpointsWithoutConnection = 0;
			int maxNumCon=0, minNumCon=Integer.MAX_VALUE, sumNumCon=0;
			
			for(EndPoint ep : problem.endpoints) {
				int n = ep.latencies.size();
				if(n==0) endpointsWithoutConnection++;
				if(n>maxNumCon) maxNumCon = n;
				if(n<minNumCon) minNumCon = n;
				sumNumCon += n;
				for(int serverId : ep.latencies.keySet()) {
					connectionsPerServer[serverId]++;
				}
			}

			sb.append(String.format("Endpoints: %d Connections: max=%d , min=%d, avg=%f, epWith0Conn=%d",
					problem.E, maxNumCon, minNumCon, sumNumCon*1f/problem.E, endpointsWithoutConnection));
			sb.append("\n");
		}
		{
			int serversWithoutConnection = 0;
			int maxServerNumCon=0, minServerNumCon=Integer.MAX_VALUE, sumServerNumCon=0;
			for(int c : connectionsPerServer) {
				if(c==0) serversWithoutConnection++;
				if(c>maxServerNumCon) maxServerNumCon = c;
				if(c<minServerNumCon) minServerNumCon = c;
				sumServerNumCon += c;
			}

			sb.append(String.format("Servers: %d Connections: max=%d , min=%d, avg=%f, serverWith0Conn=%d",
					problem.C, maxServerNumCon, minServerNumCon, sumServerNumCon*1f/problem.C, serversWithoutConnection));
			sb.append("\n");
		}
		{
			int serversWithoutVideo = 0;
			int maxServerNumVideos=0, minServerNumVideos=Integer.MAX_VALUE, sumServerNumVideos=0;
			for(CacheServer cs : servers) {
				int n = cs.videos.size();
				if(n==0) serversWithoutVideo++;
				if(n>maxServerNumVideos) maxServerNumVideos = n;
				if(n<minServerNumVideos) minServerNumVideos = n;
				sumServerNumVideos += n;
			}

			sb.append(String.format("Servers: %d num videos: max=%d , min=%d, avg=%f, serverWith0Conn=%d",
					problem.C, maxServerNumVideos, minServerNumVideos, sumServerNumVideos*1f/problem.C, serversWithoutVideo));
			sb.append("\n");
		}
		
		{
			int numRequestsWithNoPossibleGain = 0;
			int numRequestsFullySatisfied = 0;
			int numRequests90PctSatisfied = 0;
			int numRequests0PctSatisfied = 0;

			for(Request r : problem.requests) {
				r.computeBestPossibleGain(this);
				int s = r.computeRequestScore(problem);
				if(r.bestPossibleGain==0) {
					numRequestsWithNoPossibleGain++;
				}
				if(r.bestPossibleGain == s) {
					numRequestsFullySatisfied++;
				} 
				if(s >= r.bestPossibleGain*0.90f) {
					numRequests90PctSatisfied++;
				}
				if(r.bestPossibleGain>0 && s==0) {
					numRequests0PctSatisfied++;
				}
			}

			sb.append(String.format("Requests with no possible gain: %d / %d", numRequestsWithNoPossibleGain, problem.R));
			sb.append("\n");

			sb.append(String.format("Requests fully satisfied: %d / %d", numRequestsFullySatisfied, problem.R));
			sb.append("\n");
			sb.append(String.format("Requests 90 pct satisfied: %d / %d", numRequests90PctSatisfied, problem.R));
			sb.append("\n");
			sb.append(String.format("Requests 0 pct satisfied: %d / %d", numRequests0PctSatisfied, problem.R));
			sb.append("\n");
		}
		
		{
			int minVideoSize = Integer.MAX_VALUE;
			int maxVideoSize = Integer.MIN_VALUE;
			for(int videoSize : problem.videoSizes) {
				if(videoSize < minVideoSize) minVideoSize = videoSize;
				if(videoSize > maxVideoSize) maxVideoSize = videoSize;
			}

			int serversFull = 0;
			for(CacheServer cs : servers) {
				if(cs.getSpaceTaken()+minVideoSize > problem.X) {
					serversFull++;
				}
			}

			sb.append(String.format("Video size: min=%d, max=%d, servers full: %d / %d ", minVideoSize, maxVideoSize, serversFull, problem.C));
			sb.append("\n");
		}
		return sb.toString();
	}
	
	public Algo(Problem problem, String nameOfFile) {
		this.problem = problem;
		this.dataName = nameOfFile;
		
		this.servers = new ArrayList<CacheServer>();
		for(int csId = 0; csId < this.problem.C; csId++) {
			CacheServer cs = new CacheServer(csId);
			this.servers.add(cs);
		}
		
		this.initiateLossAndGains();
		
		System.out.println("initiate done");
	}
	
	public static Algo readSolution(String nameOfFile, int NALGO) throws IOException {
		return readSolution(nameOfFile, NALGO, null);
	}
	    
	
	public static Algo readSolution(String nameOfFile, int NALGO, String fileOutPath) throws IOException {
    	Algo algo = new Algo(new Problem(new File("data/input/"+nameOfFile+".in")), nameOfFile);
	    
    	File outFile = new File("data/output/manu_"+nameOfFile+"_"+NALGO+".out");
    	if(fileOutPath != null) {
    		outFile = new File(fileOutPath);
    	}
	    algo.readFromFile(outFile);
	    algo.dataName = nameOfFile;
	    boolean correct = algo.checkCorrect();  
	    int scoreFinal = algo.computeScoreFinal();
	       
	    System.out.println(String.format(
	    		"Read from: %s, Correct: %b, score read: %d",
	    		outFile.getPath(),correct,scoreFinal));
	    
	    return algo;
    }
	
	public void studyRequests() {
		 ArrayList<Request> orderedRequests = new ArrayList<Request>(problem.requests);
		    for(Request rq : orderedRequests) {
		    	rq.computeBestPossibleGain(this);
		    	rq.tmpGain = rq.computeRequestScore(problem);
		    }
		    
		    Collections.sort(orderedRequests, new Comparator<Request>() {
			    public int compare(Request a, Request b) {
			    	return Integer.compare(a.tmpGain,  b.tmpGain);
			    }
			});
		    Collections.reverse(orderedRequests);
		    
		    
		    StringBuilder sb = new StringBuilder();
		    sb.append("\nOrdered by gain");
		    for(int i=0; i<=10; i++) {
		    	int idx = orderedRequests.size()*i/10;
		    	if(idx>orderedRequests.size()-1) idx = orderedRequests.size()-1;
		    	Request rq = orderedRequests.get(idx);
		    	
		    	sb.append(String.format("\nRequest gain at %d : %d, max possible for this request: %d", 
		    			idx, rq.tmpGain, rq.bestPossibleGain));
		    }
		    
		    // order requests by max distance between current gain and max possible gain
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
		    
		    sb.append("\n\nOrdered by gain dist");
		    for(int i=0; i<=10; i++) {
		    	int idx = orderedRequests.size()*i/10;
		    	if(idx>orderedRequests.size()-1) idx = orderedRequests.size()-1;
		    	Request rq = orderedRequests.get(idx);
		    	int gain = rq.computeRequestScore(this.problem);
		    	
		    	sb.append(String.format("\nRequest gain at %d : %d, dist: %d, video size: %d", 
		    			idx, gain, rq.bestPossibleGain-gain, this.problem.videoSizes[rq.Rv]));
		    }
		    
		    System.out.println(sb);
	}
    
	// act as if servers had an infinite capacity,
	// and just maximize the gain for each request
	// Equivalently, consider that all videos are in 
	// all servers and compute the corresponding score
	public int computeUpperBoundOld(String filename) {
		
		int numRequestsNoGain = 0;
		
		for(Request request : problem.requests) {
			
			EndPoint ep = problem.endpoints.get(request.Re);
			
			if(problem.videoSizes[request.Rv] > problem.X) {
				continue; // will never be able to improve request
			}
			
			int bestLatency = ep.Ld;
			CacheServer bestServer = null;
			
			for(Map.Entry<Integer, Integer> entry : ep.latencies.entrySet()) {
				CacheServer cs = servers.get(entry.getKey());
				if( entry.getValue() < bestLatency) {
					bestLatency = entry.getValue();
					bestServer = cs;
				}
			}
			
			if(bestServer == null) {
				numRequestsNoGain++;
			} else {
				request.serverUsed = bestServer;
				if(!request.serverUsed.videos.contains(request.Rv)) {
					request.serverUsed.videos.add(request.Rv);
					request.serverUsed.spaceTaken += problem.videoSizes[request.Rv];
				}
			}
		}
		
		System.out.println(String.format(
					"Found %d/%d requests with no gain", 
					numRequestsNoGain, problem.requests.size())); 
		
		int numOverFlowServers = 0;
		int maxOverFlow = 0;
		int sumOverflows = 0;
		for(CacheServer server : servers) {
			if(server.spaceTaken > problem.X) {
				numOverFlowServers++;
				int overflow = server.spaceTaken - problem.X;
				if(overflow > maxOverFlow) maxOverFlow = overflow;
				sumOverflows += overflow;
			}
		}
		float avgOverflow = sumOverflows*1f/numOverFlowServers;
		
		System.out.println(String.format(
				"Overflow servers: %d/%d, avg overflow: %f/%d, max overflow: %d/%d", 
				numOverFlowServers, servers.size(), avgOverflow, problem.X, maxOverFlow, problem.X)); 
		
		int upperBound = computeScoreFinal();
		System.out.println(String.format("Upper bound for %s: %d", filename, upperBound));
		return upperBound;
	}
	
	public HashSet<CacheServer> getPotentialServers(int videoId) {
		return potentialServersForVideo.get(videoId);
	}
	
	public long getLossOut(CacheServer server, int videoId) {
		if(!server.videos.contains(videoId)) {
			System.out.println("video not in server");
		}
		
		//synchronized(allLossOut) {
		//	return allLossOut[server.serverId][videoId];
		//}
		return allLossOutMap.get(_hash(server, videoId));
		
		
	}
	
	public long getGainPut(CacheServer server, int videoId) {
		if(server.videos.contains(videoId)) {
			System.out.println("video already in server");
		}
		
		return allGainsPut.get(_hash(server, videoId));
	}
	
	//private long[][] allLossOut;
	
	private int _hash(CacheServer cs, int videoId) {
		return cs.serverId*problem.V + videoId;
	}
	
	public void setDirty(int videoId) {
		videoIsClean[videoId] = false;
	}
	
	private ConcurrentHashMap<Integer, Long> allLossOutMap; // (cs,videoId) -> current putPushInfo
	private ConcurrentHashMap<Integer, Long> allGainsPut; // (cs,videoId) -> current gain put
	private boolean[] videoIsClean; 
	
	//private ConcurrentHashMap<Integer, PutPushInfo> allPutPushInfo; // (cs,videoId) -> current putPushInfo
	//private HashSet<PutPushInfo>[] videoToPutPushInfo; // [videoId] -> all putPushInfo it belongs to, 
													// either as video to put or to take out
	//private boolean[] videoIsCleanForPPI;
	
	
	
	private ConcurrentHashMap<Integer, HashSet<CacheServer>> potentialServersForVideo;
	
	
	
	public void initiateLossAndGains() {
		allLossOutMap = new ConcurrentHashMap<Integer, Long>();
		allGainsPut = new ConcurrentHashMap<Integer, Long>();
		videoIsClean = new boolean[problem.V]; // starts at false
		
		
		potentialServersForVideo = new ConcurrentHashMap<Integer, HashSet<CacheServer>>();
		
		for(int videoId=0; videoId<problem.V; videoId++) {
			HashSet<CacheServer> potentialServers = new HashSet<CacheServer>(); 
			potentialServersForVideo.put(videoId, potentialServers);
		}
		
		for(Request request : problem.requests) {
			EndPoint ep = this.problem.endpoints.get(request.Re);
			for(int serverId : ep.latencies.keySet()) {
				potentialServersForVideo.get(request.Rv).add(this.servers.get(serverId));
			}
		}	
		
	}
	
	// compute the cost of taking a video out of a server for all videos and all servers
	// and the gain to put it in a server where it is not
	// re-computed only for "dirt" videos (that have had any change)
	public static void updateAllLossOutAndGainsPut(int numThreads, final Algo algo, final boolean computeLossesOnly) {
		//algo.allLossOut = new long[algo.problem.C][algo.problem.V];
		
		final AtomicInteger processed = new AtomicInteger();
		final int logEvery = Math.max(1, algo.problem.C/10);
		ExecutorService executor = Executors.newFixedThreadPool(numThreads); 
		
		// Spawn threads
		for(final CacheServer cacheServer : algo.servers) {
			executor.execute(new Runnable(){
				@Override
				public void run() {
					try {
						int numProcessed = processed.incrementAndGet();
						if(numProcessed%logEvery==0) {
							//logger.info("Computing loss outs "+numProcessed+"/"+algo.problem.C);
							//System.out.print(".");
						}
						
						for(int videoId = 0; videoId<algo.problem.V; videoId++) {
		    				if(algo.videoIsClean[videoId]) {
		    					continue; // no need to change video
		    				}
							
							if(cacheServer.videos.contains(videoId)) {
		    					long lossOut = Manu.computeLossOut(videoId, cacheServer, algo);
		    					//synchronized(algo.allLossOut) {
			    					//algo.allLossOut[cacheServer.serverId][videoId] = lossOut;
			    				//}
		    					algo.allLossOutMap.put(algo._hash(cacheServer, videoId), lossOut);
		    				} else {
		    					if(!computeLossesOnly) {
		    						long gain = Manu.computeGainPut(videoId, cacheServer, algo.problem, true);
		    						algo.allGainsPut.put(algo._hash(cacheServer, videoId), gain);
		    					}
		    				}
							
		    			}
						
					} catch (Exception e) {
						logger.log(Level.WARNING, "Failure computing loss outs, server: " + cacheServer.serverId, e);
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
		
		for(int videoId = 0; videoId<algo.problem.V; videoId++) {
			algo.videoIsClean[videoId] = true;
		}
	}
	
	public void algoFromUpperBound() {
		
		// each request has their video in the best possible server
		// but the servers overflow
		
		int numVideosOut = 0;
		
		for(CacheServer server : servers) {
			
			// first take out video that are too big by themselves
			
			if(server.getSpaceTaken() <= problem.X) {
				continue;
			}
			
			ArrayList<Video> videos = new ArrayList<Video>();
			
			for(int videoId : server.videos) {
				Video video = new Video(videoId, problem);
				video.lossOut = Manu.computeLossOut(videoId, server, this);
				videos.add(video);
			}
			
			// sort by increasing loss per byte
			Collections.sort(videos, new Comparator<Video>() {
			    public int compare(Video a, Video b) {
			    	float lossPerMbA = a.lossOut*1f/a.size;
			    	float lossPerMbB = b.lossOut*1f/b.size;
			        
			    	return Float.compare(lossPerMbA, lossPerMbB);
			    	//return Integer.compare(a.lossOut, b.lossOut);
			    	//return Integer.compare(a.size, b.size);
			    }
			});
			
			
			int idx = 0;
			while(server.getSpaceTaken() > problem.X) {
				// take out the video that implies the smallest score loss (per MB of video size)
				idx++;
				server.removeVideoAndUpdateRequests(videos.get(idx).videoId, this);
				numVideosOut++;
				
			}
		}
		
		System.out.println("Videos out: " + numVideosOut);
		
	}
	
	public static void main(String[] args) throws IOException {
		int max = 0;
		
		String[] strings = new String[]{"example", "me_at_the_zoo", "trending_today", "videos_worth_spreading", "kittens"};
		
		for(String s : new String[]{"videos_worth_spreading"}){
			System.out.println("\n*********************************************\n");
			
			Algo algo = readSolution(s, 62);
			//algo.studyRequests();
			System.out.println(algo.getCurretInfo());
			
			if(!s.equals("me_at_the_zoo")) {
				max+=algo.computeUpperBound2();
			}
			
			
			/*
			algo.algoFromUpperBound();
			Manu.DoAlgo1(algo, Integer.MAX_VALUE, 0, 0);
			
			boolean correct = algo.checkCorrect();  
			System.out.println(String.format("Correct: %b", correct));
			int scoreFinal = algo.computeScoreFinal();
			System.out.println(String.format("Score final: %d", scoreFinal));
			*/
		}
		System.out.println("max: " + max);
	}
}
