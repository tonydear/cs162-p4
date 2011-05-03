package edu.berkeley.cs.cs162;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class ServerConnection extends Thread {
	private ObjectOutputStream oos;
	private ObjectInputStream ios;
	private Socket mySocket;
	
	public ServerConnection(Socket socket){
		mySocket = socket;
		try {
			ios = new ObjectInputStream(socket.getInputStream());
			oos = new ObjectOutputStream(socket.getOutputStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void forwardMessage(TransportObject sendMe){ //send things out
		
	}
	
	public void run(){ //listen for messages
		
	}

}
