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
	HashMap<Integer, CacheServer> caches; // cache server id (0<=c<C) -> list of video ids (0<=v<V)
	
	public void printToFile(File file) throws IOException {
		FileOutputStream fos = new FileOutputStream(file);
	 	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
	 
		bw.write(Integer.toString(caches.size()));
		bw.newLine();
		for(Map.Entry<Integer, CacheServer> entry : caches.entrySet()) {
			StringBuilder sb =new StringBuilder();
			sb.append(entry.getKey());
			
			for(Integer videoId : entry.getValue().videos) {
				sb.append(" "+videoId);
			}
			
			
			bw.write(sb.toString());
			bw.newLine();
		}
	 	bw.close();
	 	
	 	System.out.println(String.format("Saved at: %s", file.getPath()));
	}
	
	public boolean checkCorrect() {
		
		for(Map.Entry<Integer, CacheServer> entry : caches.entrySet()) {
			
			CacheServer cs = entry.getValue();
			
			int sizeInServer = 0;
			
			for(Integer videoId : cs.videos) {
				sizeInServer += problem.videoSizes[videoId];
			}
			
			if(sizeInServer > problem.X) {
				System.out.println("Too much at server: " + cs.serverId);
			}
			
		}
		
		return true;
	}
	
	public int computeScore() {
		
		int score = 0;
		int numRequests = 0;
		
		for(Request request : problem.requests) {
			int rScore = computeRequestScore(request);
			
			score += rScore;
			numRequests += request.Rn;
		}
		
		return (score * 1000) / numRequests;
		
	}
	
	public int computeRequestScore(Request request) {
		
		// find the cache servers that have the video and are connected to the endpoint
		EndPoint endpoint = problem.endpoints.get(request.Re);
		
		int minLatency = endpoint.Ld; // latency to datacenter
		CacheServer bestCacheServer = null;
		
		for(CacheServer cs : caches.values()) {
			
			boolean connected = endpoint.latencies.containsKey(cs.serverId);
			boolean serverHasVideo = cs.videos.contains(request.Rv);
			
			if(connected && serverHasVideo) {
				int latency = endpoint.latencies.get(cs.serverId);
				if(latency < minLatency) {
					minLatency = latency;
					bestCacheServer = cs;
				}
			}
		}
		
		
		//System.out.println(String.format("latency: %d, server: %d", minLatency, bestCacheServer==null ? -1 : bestCacheServer.serverId ));
		
		int score = request.Rn * (endpoint.Ld - minLatency);
		
		System.out.println(String.format("Score: %d, server: %d", score, bestCacheServer==null ? -1 : bestCacheServer.serverId ));
		
		return score;
	}
	
	
	
	
	
	
}
