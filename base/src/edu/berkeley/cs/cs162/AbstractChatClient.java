package edu.berkeley.cs.cs162;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public abstract class AbstractChatClient extends Thread{
	private Socket mySocket;
	private Map<String,ChatLog> logs;
	private BufferedReader commands;
	private ObjectInputStream received;
	protected ObjectOutputStream sent;
	private Thread receiver;
	protected volatile boolean connected;
	protected Command reply; 				//what reply from server should look like
	protected volatile boolean isWaiting; //waiting for reply from server?
	protected volatile boolean isLoggedIn;
	private volatile boolean isQueued;
	private String homeIP;
	private String backupIP;
	private int homePort;
	private int backupPort;
	private Thread homePoller;
	protected String password;
	protected String username;
	private boolean connectedToHome;
	private boolean printLogAck;
	private volatile boolean backupVar;
	private volatile boolean returnVar;
	
	public AbstractChatClient(){
		mySocket = null;
		logs = new HashMap<String,ChatLog>();
		commands = new BufferedReader(new InputStreamReader(System.in));
		connected = false;
		isWaiting = false;
		reply = null;
		receiver = null;
		printLogAck = true;
		backupVar = false;
		returnVar = false;
        start();
	}
	
	private void connect(String hostname, int port){
		try {
			if (connected) {
				System.err.println("already connected");
				return;
			}
			mySocket = new Socket(hostname,port);
			sent = new ObjectOutputStream(mySocket.getOutputStream());
			InputStream input = mySocket.getInputStream();
			received = new ObjectInputStream(input);
			
			connected = true;
			output("connect OK");
			if(receiver == null || !receiver.isAlive()) {
				receiver = new Thread(){
		            @Override
		            public void run(){
		            	while(connected){
		            		System.out.println("about to begin receive");
		            		receive();
		            		System.out.println("finished receive");
		            	}
		            }
		        };
				receiver.start();
			}
		} catch (IllegalThreadStateException e) {
			e.printStackTrace();
		} catch (Exception e) {
			output("connect REJECTED");
			e.printStackTrace();
		}
	}
	
	private void disconnect(){
		if(!connected)
			return;
		try {
			mySocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(isLoggedIn)
			output("logout OK");
		output("disconnect OK");
		isLoggedIn = false;
		isQueued = false;
		connected = false;
	}
	
	private void output(String o){
		System.out.println(o);
	}
	
	private void login(String user, String pass){
		try {
			if (connected) {
				System.err.println("already connected");
				return;
			}

			this.username = user;
			this.password = pass;
			List<String> serverNames = DBHandler.getServerNames(username);
			homeIP = DBHandler.getIP(serverNames.get(0));
			homePort = DBHandler.getClientPort(serverNames.get(0));
			backupIP = DBHandler.getIP(serverNames.get(1));
			backupPort = DBHandler.getClientPort(serverNames.get(1));
			System.out.println(homeIP + " " + homePort);
			try {
				mySocket = new Socket(homeIP, homePort);
				connectedToHome = true;
			} catch(IOException e) {
				try{
					mySocket = new Socket(backupIP, backupPort);
					connectedToHome = false;
					homePoller = new Thread() {
						private Socket homeSocket;
						
						
						@Override
						public void run(){
							while(true) {
								synchronized(AbstractChatClient.this){
									//System.out.println("new polling");
									try {
										homeSocket = new Socket(homeIP, homePort);
										TransportObject toSend = new TransportObject(Command.logout);
										try {
											sent.writeObject(toSend);
											printLogAck = false;
											isWaiting = true;
											reply = Command.logout;
											AbstractChatClient.this.wait();
										} catch (Exception e) {
											e.printStackTrace();
										}
										mySocket = homeSocket;
										sent = new ObjectOutputStream(mySocket.getOutputStream());
										InputStream input = mySocket.getInputStream();
										received = new ObjectInputStream(input);
										if (!connected && (!isLoggedIn || !isQueued))
											return;
										TransportObject toSendLogin = new TransportObject(Command.login, username, password);
										try {
											isWaiting = true;
											reply = Command.login;
											sent.writeObject(toSendLogin);
											AbstractChatClient.this.wait();
										} catch (Exception e) {

										}
										connectedToHome = true;
										break;
									} catch (UnknownHostException e1) {
										// TODO Auto-generated catch block
										//e1.printStackTrace();
									} catch (IOException e1) {
										// TODO Auto-generated catch block
										//e1.printStackTrace();
									}
								}
								try {
									sleep(1000);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						}
					};
					homePoller.start();
				} catch(IOException ex){
					output("login REJECTED");
				}
			}
			
			sent = new ObjectOutputStream(mySocket.getOutputStream());
			InputStream input = mySocket.getInputStream();
			received = new ObjectInputStream(input);
			
			connected = true;
			if(receiver == null || !receiver.isAlive()) {
				receiver = new Thread(){
		            @Override
		            public void run(){
		            	while(connected){
		            		System.out.println("about to receive");
		            		receive();
		            		System.out.println("finished receiving");
		            	}
		            }
		        };
				receiver.start();
			}
		} catch (IllegalThreadStateException e) {
			e.printStackTrace();
		} catch (Exception e) {
			output("connect REJECTED");
			e.printStackTrace();
		}
		
		if (!connected && (!isLoggedIn || !isQueued))
			return;
		TransportObject toSend = new TransportObject(Command.login, username, password);
		try {
			isWaiting = true;
			reply = Command.login;
			System.out.println("sending initial login obj");
			sent.writeObject(toSend);
			this.wait();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void logout(){
		if (!connected || (!isLoggedIn && !isQueued))
			return;
		TransportObject toSend = new TransportObject(Command.logout);
		try {
			sent.writeObject(toSend);
			isWaiting = true;
			reply = Command.logout;
			this.wait();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void join(String gname){
		if(!connected || !isLoggedIn)
			return;
		TransportObject toSend = new TransportObject(Command.join,gname);
		try {
			isWaiting = true;
			reply = Command.join;
			sent.writeObject(toSend);
			this.wait();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return;
	}
	
	private void leave(String gname){
		if(!connected || !isLoggedIn)
			return;
		TransportObject toSend = new TransportObject(Command.leave,gname);
		try {
			isWaiting = true;
			reply = Command.leave;
			sent.writeObject(toSend);
			this.wait();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return;
	}
	
	protected abstract void send(String dest, int sqn, String msg);
	
	private synchronized void switchToBackup() {
		connected = false;
		System.out.println("home went down! switching to backup beep beep");
		if (connectedToHome) {
			try {
				mySocket = new Socket(backupIP, backupPort);
				sent = new ObjectOutputStream(mySocket.getOutputStream());
				InputStream input = mySocket.getInputStream();
				received = new ObjectInputStream(input);
				backupVar = false;
				connected = true;
				if (!connected && (!isLoggedIn || !isQueued))
					return;
				TransportObject toSend = new TransportObject(Command.login, username, password);
				try {
					isWaiting = true;
					reply = Command.login;
					sent.writeObject(toSend);
					this.wait();
				} catch (Exception e) {
					e.printStackTrace();
				}
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				System.err.println("backup server connection failed");
				e1.printStackTrace();
			} 
			System.out.println("about to create homepoller thread");
			homePoller = new Thread() {
				private Socket homeSocket;
				@Override
				public void run(){
					System.out.println("starting run of home poller");
					while(true) {
						
						synchronized(AbstractChatClient.this){
							try {
						
								homeSocket = new Socket(homeIP, homePort);
								returnVar = true;
								TransportObject toSend = new TransportObject(Command.logout);
								try {
									sent.writeObject(toSend);
									isWaiting = true;
									reply = Command.logout;
									AbstractChatClient.this.wait();
								} catch (Exception e) {
									e.printStackTrace();
								}
								mySocket = homeSocket;
								sent = new ObjectOutputStream(mySocket.getOutputStream());
								InputStream input = mySocket.getInputStream();
								received = new ObjectInputStream(input);
								connected = true;
								connectedToHome = true;
								if (!connected && (!isLoggedIn || !isQueued))
									return;
								TransportObject toSendLogin = new TransportObject(Command.login, username, password);
								try {
									isWaiting = true;
									reply = Command.login;
									sent.writeObject(toSendLogin);
									System.out.println("about to wait");
									receiver = new Thread(){
							            @Override
							            public void run(){
							            	while(connected){
							            		System.out.println("about to receive");
							            		receive();
							            		System.out.println("finished receiving");
							            	}
							            }
							        };
									receiver.start();
									
								} catch (Exception e) {
									e.printStackTrace();
								}
								break;
							} catch (UnknownHostException e1) {
								// TODO Auto-generated catch block
								//e1.printStackTrace();
							} catch (IOException e1) {
								// TODO Auto-generated catch block
								//e1.printStackTrace();
							}
						}
					
						try {
							sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			};
			homePoller.start();
		} else {
			connected = false;
			System.err.println("backup server connection failed");
		}
	}
	
	private void receive(){
		TransportObject recObject = null;
		try {
			Object o = received.readObject();
			//if(o!=null)
				//System.out.println(o);
			recObject = (TransportObject) o;
		} catch (SocketException e) {
			connected = false;
			return;
		} catch (EOFException e) {
			Thread backupThread = new Thread() {
				public void run() {
					switchToBackup();
				}
			};
			backupVar = true;
			backupThread.start();
			while(backupVar) {
				
			}
			System.out.println("Finished switching to backup");
			
			return;
		}
		catch (Exception e) {
			e.printStackTrace();
			connected = false;
			return;
		}
		
		if (recObject == null){
			return;
		}
		Command type = recObject.getCommand();
		System.out.println("type " + type + " " + isWaiting);
		System.out.println("serverreply " + recObject.getServerReply());
		System.out.println("message : " + recObject.getMessage());
		ServerReply servReply = recObject.getServerReply();
		if (servReply.equals(ServerReply.error)) {
			System.err.println("Error");
		} else if (isWaiting && type.equals(reply)) {
			if (reply.equals(Command.disconnect) || reply.equals(Command.login) || 
					reply.equals(Command.logout) || reply.equals(Command.adduser)) {
				if(reply.equals(Command.logout)) {
					connected = false;
					isLoggedIn = false;
						
				}
				if(reply.equals(Command.login) || reply.equals(Command.logout)) {
					if(reply.equals(Command.login)) {
						System.out.println("setting returnVar to false");
						returnVar = false;
					}
					if(printLogAck) {
						output(type.toString() + " " + servReply.toString());
					} else {
						if(reply.equals(Command.login)) {
							printLogAck = true;
						}
					}
				} else {
					output(type.toString() + " " + servReply.toString());
				}
				if (reply.equals(Command.disconnect)){
					connected = false;
					isLoggedIn = false;
					isQueued = false;
				}
				else if (reply.equals(Command.login)){
					if(recObject.getServerReply().equals(ServerReply.OK)){
						isQueued = false;
						isLoggedIn = true;
					}else if(recObject.getServerReply().equals(ServerReply.QUEUED)){
						isQueued = true;
					}
				}else if (reply.equals(Command.logout)){
					isLoggedIn = false;
					isQueued = false;
				}
			}
			else if (reply.equals(Command.join) || reply.equals(Command.leave))
				output(type.toString() + " " + recObject.getGname() + " " + servReply.toString());
			else if (reply.equals(Command.send))
				output(type.toString() + " " + recObject.getSQN() + " " + servReply.toString());
			else
				return;
			
			isWaiting = false;
			reply = null;
			synchronized(this) { this.notify(); }
		} 
		
		else if (servReply.equals(ServerReply.sendack)) {
			output(servReply.toString() + " " + recObject.getSQN() + " FAILED");
			if(isWaiting) {
				isWaiting = false;
				reply = null;
				synchronized(this) { this.notify(); }
			}
		}
		else if (servReply.equals(ServerReply.receive)) {
			benchmark(recObject);
			//output(servReply.toString() + " " + recObject.getTimestamp() + " " +
					//recObject.getSender() + " " + recObject.getDest() + " " + recObject.getMessage());
			System.out.format(servReply.toString() + " %.3f " + recObject.getSender() + " " + recObject.getDest() + 
					" " + recObject.getMessage() + "\n", Double.parseDouble(recObject.getTimestamp()));
		}
		else if (servReply.equals(ServerReply.timeout)) {
			output(servReply.toString());
			connected = false;
		}else if (type.equals(Command.login) && isQueued){
			if (printLogAck) {
				output("login OK");
			}
			isQueued = false;
			isLoggedIn = true;
		} else {
			System.out.println("What kind of server reply is this? " + servReply);
		}
	}
	
	private void sleep(int time){
		try {
			output("sleep OK");
			Thread.sleep(time);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private void readlog(){
		TransportObject toSend = new TransportObject(Command.readlog);
		try{
			sent.writeObject(toSend);
		} catch (Exception e){
			connected = false;
			isLoggedIn = false;
			isQueued = false; //should be already
			e.printStackTrace();
		}
		//System.out.println("sent readlog request, connect: " + connected);
	}
	
	public Map<String,ChatLog> getLogs(){
		return logs;
	}
	
	public void processCommands() throws Exception {
		int x = 0;
		System.out.println("x is " + x + " befor eloop");
		while(returnVar){
			x=1;
		}
		System.out.println("x is " + x);
		String command = retrieveCommand();
		if(command == null)
			return;
		String[] tokens = command.split(" ");
		int args = tokens.length;
		if (tokens.length == 0)
			return;
		synchronized(this){
			if (tokens[0].equals("connect")){
				if(args != 2)
					throw new Exception("invalid arguments for connect command");
				tokens = tokens[1].split(":");
				args = tokens.length;
				if (args != 2)
					throw new Exception("invalid arguments for connect command");
				String hostname = tokens[0];
				int port;
				try {
					port = Integer.parseInt(tokens[1]);
				} catch (NumberFormatException e) {
					e.printStackTrace();
					return;
				}
				connect(hostname, port);
			}
			else if (tokens[0].equals("disconnect")) {
				if (args != 1)
					throw new Exception("invalid arguments for disconnect command");
				disconnect();
			}
			else if (tokens[0].equals("login")) {
				if (args != 3)
					throw new Exception("invalid arguments for login command");
				username = tokens[1];
				password = tokens[2];
				login(username,password);
			}

			else if (tokens[0].equals("logout")) {
				if(args != 1)
					throw new Exception("invalid arguments for logout command");
				logout();
			}
			else if (tokens[0].equals("join")) {
				if(args != 2)
					throw new Exception("invalid arguments for join command");
				String gname = tokens[1];
				join(gname);
			}
			else if (tokens[0].equals("leave")) {
				if(args != 2)
					throw new Exception("invalid arguments for leave command");
				String gname = tokens[1];
				leave(gname);
			}
			else if (tokens[0].equals("send")) {
				if(args < 4)
					throw new Exception("invalid arguments for send command");
				tokens = command.split(" ",4);
				String dest = tokens[1];
				int sqn;
				try {
					sqn = Integer.parseInt(tokens[2]);
				} catch (NumberFormatException e) {
					e.printStackTrace();
					return;
				}
				String msg = tokens[3];
				send(dest,sqn,msg);
			}
			else if (tokens[0].equals("sleep")) {
				if(args != 2)
					throw new Exception("invalid arguments for sleep command");
				int time;
				try {
					time = Integer.parseInt(tokens[1]);
				} catch (NumberFormatException e) {
					e.printStackTrace();
					return;
				}
				sleep(time);
			}
			else if (tokens[0].equals("readlog")){
				if(args!=1)
					throw new Exception("invalid arguments for readlog command");
				readlog();
			}

			else {
				throw new Exception("invalid command");
			}
		}
	}

	protected String retrieveCommand() {
		String command = null;
		try {
			command = commands.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return command;
	}
	
	@Override
	public void run(){
		while(true){
			try {
				processCommands();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	protected abstract void benchmark(TransportObject recObject);
}
