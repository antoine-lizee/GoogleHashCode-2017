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

public class Algo {
	
	Problem problem;
	
	ArrayList<CacheServer> servers;
	//HashMap<Integer, HashSet<CacheServer>> videoIdToPotentialServers;
	
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
	 				this.servers.get(serverId).putVideo(videoId, problem);
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
				System.out.println("Too much at server: " + cs.serverId);
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
	
	public HashSet<CacheServer> getPotentialServers(int videoId) {
		HashSet<CacheServer> potentialServers = new HashSet<CacheServer>(); 
		for(Request request : this.problem.videoIdToRequests.get(videoId)) {
			EndPoint ep = this.problem.endpoints.get(request.Re);
			for(int serverId : ep.latencies.keySet()) {
				potentialServers.add(this.servers.get(serverId));
			}
		}	
		return potentialServers;
	}
	
	public Algo(Problem problem) {
		this.problem = problem;
		
		this.servers = new ArrayList<CacheServer>();
		for(int csId = 0; csId < this.problem.C; csId++) {
			CacheServer cs = new CacheServer(csId);
			this.servers.add(cs);
		}
		
		/*
		videoIdToPotentialServers = new HashMap<Integer, HashSet<CacheServer>>();
		for(int videoId = 0; videoId < this.problem.V; videoId++) {
			HashSet<CacheServer> potentialServers = new HashSet<CacheServer>(); 
			videoIdToPotentialServers.put(videoId, potentialServers);
		}
		
		for(int videoId = 0; videoId < this.problem.V; videoId++) {
			for(Request request : this.problem.videoIdToRequests.get(videoId)) {
				EndPoint ep = this.problem.endpoints.get(request.Re);
				for(int serverId : ep.latencies.keySet()) {
					//this.servers.get(serverId).potentialVideos.add(videoId);
					videoIdToPotentialServers.get(videoId).add(this.servers.get(serverId));
				}
			}	
		}*/
		
		
		System.out.println("initiate done");
	}
	
	public static Algo readSolution(String nameOfFile, int NALGO) throws IOException {
    	Algo algo = new Algo(new Problem(new File("data/input/"+nameOfFile+".in")));
	    File outFile = new File("data/output/manu_"+nameOfFile+"_"+NALGO+".out");
	    algo.readFromFile(outFile);
	    
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
	public int computeUpperBound(String filename) {
		
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
				video.lossOut = Manu.lossOut(videoId, server, this);
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
				server.removeVideoAndUpdateRequests(videos.get(idx).id, this);
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
			
			Algo algo = readSolution(s, 20);
			algo.studyRequests();
			int upperBound = algo.computeUpperBound(s);	
			if(!s.equals("example")) {
				max+=upperBound;
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
