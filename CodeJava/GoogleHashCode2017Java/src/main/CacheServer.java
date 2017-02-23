package main;

import java.util.HashSet;

public class CacheServer {

	int serverId;
	HashSet<Integer> videos; // ids of videos stored in this server
	
	public CacheServer(int id) {
		serverId = id;
		videos = new HashSet<Integer>();
	}
}
