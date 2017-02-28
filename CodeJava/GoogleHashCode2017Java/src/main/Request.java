package main;

import java.util.Map;

public class Request {
	int Rv, Re, Rn;
	int id;
	
	CacheServer serverUsed;
	
	public int bestPossibleGain = -1;
	public int tmpGain;
	
	// current gain for this request
	public int computeRequestScore(Problem problem) {
		if(serverUsed == null) {
			return 0;
		} else {
			EndPoint endpoint = problem.endpoints.get(Re);
			return Rn * (endpoint.Ld - endpoint.latencies.get(serverUsed.serverId));
		}
	}
	
	public void computeBestPossibleGain(Algo algo) {
		if(this.bestPossibleGain >= 0) {
			return; // already computed
		}
		
    	EndPoint ep = algo.problem.endpoints.get(Re);
		
		if(algo.problem.videoSizes[Rv] > algo.problem.X) {
			this.bestPossibleGain = 0; // will never be able to improve request
			return;
		}
		
		int bestLatency = ep.Ld;
		//CacheServer bestServer = null;
		
		for(Map.Entry<Integer, Integer> entry : ep.latencies.entrySet()) {
			//CacheServer cs = algo.servers.get(entry.getKey());
			if( entry.getValue() < bestLatency) {
				bestLatency = entry.getValue();
				//bestServer = cs;
			}
		}
		
		this.bestPossibleGain = Rn * (ep.Ld - bestLatency);
		
	}
}
