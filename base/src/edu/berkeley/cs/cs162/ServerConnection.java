package edu.berkeley.cs.cs162;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ServerConnection{
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
		toSend = new ArrayBlockingQueue<TransportObject>(MAX_SEND);
		try {
			oos = new ObjectOutputStream(socket.getOutputStream());
			ois = new ObjectInputStream(socket.getInputStream());
			isUp = true;
			TransportObject myNameIs = new TransportObject(server.getServername());
			toSend.add(myNameIs);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String getName(){
		return name;
	}
	
	public void acceptMessage(TransportObject sendMe){ //send things out
		toSend.add(sendMe);
	}
	
	public void setup(){ 
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
			System.out.println(name + " server is connected");
		} else if (recObject.getCommand()==Command.send &&recObject.getServerReply()==ServerReply.NONE){
			User dstUser = (User) server.getUser(recObject.getDest());
			Message newMsg = new Message(recObject.getTimestamp(),recObject.getSender(),recObject.getDest(),recObject.getMessage());
			if(dstUser==null){
				try {
					DBHandler.writeLog(newMsg, recObject.getDest());
				} catch (SQLException e) {
					TestChatServer.logChatServerDropMsg(newMsg.getContent(), new Date());
					e.printStackTrace();
				}
			} else{
				dstUser.acceptMsg(newMsg);
			}
		} else if (recObject.getCommand()==Command.send) {
			User dstUser = (User) server.getUser(recObject.getDest());
			if(dstUser!=null){
				dstUser.queueReply(recObject);
			}
		}
	}
	
	/**
	 * call if connection died
	 */
	private void deleteSelf(){
		isUp = false;
		System.out.println(name + " server went down");
		if(name!=null){
			server.removeServer(name);
		}
	}
}
