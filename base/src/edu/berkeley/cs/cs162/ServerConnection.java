package edu.berkeley.cs.cs162;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ServerConnection extends Thread {
	private ObjectOutputStream oos;
	private ObjectInputStream ios;
	private Socket mySocket;
	private BlockingQueue<TransportObject> toSend;
	private final static int MAX_SEND = 10000;
	private Thread receiver; 
	private Thread sender;
	
	public ServerConnection(Socket socket){
		mySocket = socket;
		try {
			ios = new ObjectInputStream(socket.getInputStream());
			oos = new ObjectOutputStream(socket.getOutputStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		toSend = new ArrayBlockingQueue<TransportObject>(MAX_SEND);
	}
	
	private boolean queueSend(TransportObject msg) {
		return toSend.add(msg);
	}
	
	public void forwardMessage(TransportObject sendMe){ //send things out
		queueSend(sendMe);
	}
	
	public void run(){ //listen for messages
		
	}

}
