package edu.berkeley.cs.cs162;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ServerConnection extends Thread {
	private ChatServer server;
	private ObjectOutputStream oos;
	private ObjectInputStream ois;
	private String name;
	private BlockingQueue<TransportObject> toSend;
	private Thread sender, receiver;
	private volatile boolean isUp;
	private final static int MAX_SEND = 10000;
	
	public ServerConnection(Socket socket, ChatServer server){
		this.server = server;
		try {
			ois = new ObjectInputStream(socket.getInputStream());
			oos = new ObjectOutputStream(socket.getOutputStream());
			isUp = true;
			TransportObject myNameIs = new TransportObject(server.getServername());
			toSend.add(myNameIs);
		} catch (IOException e) {
			e.printStackTrace();
		}
		toSend = new ArrayBlockingQueue<TransportObject>(MAX_SEND);
	}
	

	public void acceptMessage(TransportObject sendMe){ //send things out
		toSend.add(sendMe);
	}
	
	public void run(){ 
		sender = new Thread(){
			@Override
			public void run(){
				while(isUp) {
					TransportObject msg = null;
					try {
						msg = toSend.poll(3, TimeUnit.SECONDS);
						if(msg != null)
							oos.writeObject(msg);
					} catch (SocketException e) {
						System.err.println(e);
						deleteSelf();
					} catch (Exception e) {
						if(msg.getCommand().equals(Command.send)) {
							User sender = (User) server.getUser(msg.getSender());
							if(sender!=null){
								TransportObject error = new TransportObject(ServerReply.sendack,msg.getSQN());
								sender.queueReply(error);
							}
						}
						deleteSelf();
					}
				}
			}
			
		};
		receiver = new Thread(){
			@Override
			public void run(){
				while(isUp){
					try {
						TransportObject recObject = (TransportObject) ois.readObject();
						handleReceived(recObject);
					} catch (SocketException e) {
						deleteSelf();
					} catch (EOFException e) {
						deleteSelf();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		};
		receiver.start();
		sender.start();
	}
	
	private void handleReceived(TransportObject recObject) {
		if(recObject==null) {
			isUp = false;
			return;
		}
		
		if(recObject.getServername()!=null) {
			name = recObject.getServername();
			server.addServer(name, this);
		}
		
		//TODO finish this, need to handle messages, msg failures, etc.
	}
	
	/**
	 * call if connection died
	 */
	private void deleteSelf(){
		isUp = false;
		if(name!=null){
			server.removeServer(name);
		}
	}
}
