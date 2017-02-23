package main;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class Manu {

	public static void DoAlgo0(Algo algo) {
		
		algo.caches = new HashMap<Integer, CacheServer>();
		
		// video 0
		CacheServer alloc0 = new CacheServer(0);
		alloc0.videos.add(2);
		algo.caches.put(alloc0.serverId, alloc0);
		
		// video 1
		CacheServer alloc1 = new CacheServer(1);
		alloc1.videos.add(1);
		alloc1.videos.add(3);
		algo.caches.put(alloc1.serverId, alloc1);
		
		// video 2
		CacheServer alloc2 = new CacheServer(2);
		alloc2.videos.add(0);
		alloc2.videos.add(1);
		algo.caches.put(alloc2.serverId, alloc2);
		
	}
	
    public static void doIt(String nameOfFile) throws IOException {
    	Algo algo = new Algo();
    	algo.problem = new Problem(new File("data/input/"+nameOfFile+".in"));
	    algo.caches = new HashMap<Integer, CacheServer>();
	    
	    File outFile = new File("data/output/manu_"+nameOfFile+"_0.out");
	    
	    DoAlgo0(algo);
	    
	    boolean correct = algo.checkCorrect();  
	    System.out.println(String.format("Correct: %b", correct));
	    int score = algo.computeScore();
	    System.out.println(String.format("Score: %d", score));
	    
	    algo.printToFile(outFile);
    }
    
	public static void main(String[] args) throws IOException {
		//doIt("kittens");
		//doIt("me_at_the_zoo");
		doIt("example");
		//doIt("trending_today");
		//doIt("videos_worth_spreading");
		
	}
	
}
