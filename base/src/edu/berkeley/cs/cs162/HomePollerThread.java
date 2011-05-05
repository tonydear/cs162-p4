package edu.berkeley.cs.cs162;

import java.net.Socket;

public class HomePollerThread extends Thread {
	private Socket homeSocket;
	
	private String username;
	private String password;
	public HomePollerThread(Socket homeSocket, String username, String password) {
		super();
		this.homeSocket = homeSocket;
		this.username = username;
		this.password = password;
	}
	
	public void run() {
		
	}
}
