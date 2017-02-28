package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Problem {
	
	//private static final Logger LOGGER = Logger.getLogger(Algo.class.getName());
	
		int V, E, R, C, X;
		
		int[] videoSizes; // of size V
		ArrayList<EndPoint> endpoints;
		HashSet<Request> requests;
		HashMap<Integer, ArrayList<Request>> videoIdToRequests; // all the requests that need this video
		
	    public Problem(File file) throws IOException {
			
			FileInputStream fstream = new FileInputStream(file);
			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

			int numLine = 0;
			String strLine;

			while ((strLine = br.readLine()) != null)   {
			  if(numLine==0) {
				  String[] ss = strLine.split(" ");
				  V = Integer.parseInt(ss[0]);
				  E = Integer.parseInt(ss[1]);
				  R = Integer.parseInt(ss[2]);
				  C = Integer.parseInt(ss[3]);
				  X = Integer.parseInt(ss[4]);
				  
				  videoSizes = new int[V];
				  endpoints = new ArrayList<EndPoint>();
				  requests = new HashSet<Request>();
			  } else if(numLine==1){
				  String[] ss = strLine.split(" ");
				  for(int i=0; i<ss.length; i++) {
					  videoSizes[i] = Integer.parseInt(ss[i]);
				  }
			  } else {
				  
				  if(endpoints.size() < E) {
					  
					  EndPoint endpoint = new EndPoint();
					  
					  String[] ss = strLine.split(" ");
					  endpoint.Ld = Integer.parseInt(ss[0]);
					  endpoint.K = Integer.parseInt(ss[1]);
					  endpoint.latencies = new HashMap<Integer, Integer>();
							  
					  while (endpoint.latencies.size()<endpoint.K)   {
						  strLine = br.readLine();
						  String[] ss2 = strLine.split(" ");
						  int c = Integer.parseInt(ss2[0]);
						  int Lc = Integer.parseInt(ss2[1]);
						  endpoint.latencies.put(c, Lc);
						  numLine++;
					  }
					  
					  endpoints.add(endpoint);
					  //System.out.println(endpoint.K + " " + endpoint.latencies.size());
					  
				  }
				  
				  else if(requests.size() < R) {
					  Request request = new Request();
					  String[] ss = strLine.split(" ");
					  request.Rv = Integer.parseInt(ss[0]);
					  request.Re = Integer.parseInt(ss[1]);
					  request.Rn = Integer.parseInt(ss[2]);
					  request.id = requests.size();
					  requests.add(request);
				  }
				  
				
			  }
			  numLine++;
			}

			//Close the input stream
			br.close();
			
			videoIdToRequests = new HashMap<Integer, ArrayList<Request>>();
			for(int videoId = 0; videoId<this.V; videoId++) {
				videoIdToRequests.put(videoId,  new ArrayList<Request>());
			}
			for(Request request : requests) {
				videoIdToRequests.get(request.Rv).add(request);
			}
			
		}
		
	    public String toString() {
			StringBuilder sb =new StringBuilder();
			sb.append(String.format("V=%d, E=%d, R=%d, C=%d, X=%d", V, E, R, C, X));
			sb.append("\n");
			sb.append(String.format("endpoints=%d, requests=%d", endpoints.size(), requests.size()));
			
			return sb.toString();
		}
		
	    public static void main(String[] args) throws IOException {
			File f = new File("data/input/trending_today.in");
			Problem problem = new Problem(f);
			System.out.println(problem);
		}
}
