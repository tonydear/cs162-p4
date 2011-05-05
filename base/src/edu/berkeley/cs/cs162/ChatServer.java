package edu.berkeley.cs.cs162;


import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This is the core of the chat server.  Put the management of groups
 * and users in here.  You will need to control all of the threads,
 * and respond to requests from the test harness.
 *
 * It must implement the ChatServerInterface Interface, and you should
 * not modify that interface; it is necessary for testing.
 */

public class ChatServer extends Thread implements ChatServerInterface {

	private Map<String, User> users;
	private Map<String, ChatGroup> groups;
	private Map<String, ServerConnection> servers;
	private Set<String> onlineNames;
	private ReentrantReadWriteLock lock;
	private volatile boolean isDown;
	private ServerSocket mySocket;
	private ServerSocket serverSockets;
	private Thread listenForServers;
	private String servername = null;
	
	public ChatServer() {
		users = new HashMap<String, User>();
		groups = new HashMap<String, ChatGroup>();
		servers = new ConcurrentHashMap<String, ServerConnection>();
		onlineNames = new HashSet<String>();
		lock = new ReentrantReadWriteLock(true);
		isDown = false;
	}
	
	public ChatServer(int c_port) throws IOException {
		this();
		try {
			mySocket = new ServerSocket(c_port);
		} catch (Exception e) {
			throw new IOException("Server socket creation failed");
		}
		try {
			initStructures();
		} catch (Exception e){
			e.printStackTrace();
			return;
		}
	}
	
	public ChatServer(String name, int c_port, int s_port) throws IOException {
		this(c_port);
		serverSockets = new ServerSocket(s_port);
		listenForServers = new Thread(){
			@Override
			public void run(){
				while(!isDown){
					Socket newSocket;
					try {
						newSocket = serverSockets.accept();
						ServerConnection newServer = new ServerConnection(newSocket,ChatServer.this);
						newServer.start();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		};
		servername = name;
		this.start();
	}
	
	private void initStructures() throws Exception {
		initServerConnections();
		
		//initialize groups as well as add group names to onlineNames
		ResultSet Groupnames = DBHandler.getGroups();
		while(Groupnames.next()) {
			String g = Groupnames.getString("gname");
			onlineNames.add(g);
			groups.put(g, new ChatGroup(g, this));
		}
		
		ResultSet Members = DBHandler.getMemberships();
		while(Members.next()) {
			String u = Members.getString("username"); 
			String g = Members.getString("gname");
			ChatGroup group = groups.get(g);
			if(group != null)
				group.addUser(u);
		}
	}
	
	private void initServerConnections(){
		try {
			ResultSet serverRows = DBHandler.getServers();
			while(serverRows.next()){
				String name = serverRows.getString("name");
				if(name.equals(servername)) continue;
				String ip = serverRows.getString("host");
				int port = serverRows.getInt("port");
				Socket s = new Socket(ip,port);
				ServerConnection conn = new ServerConnection(s,this);
				conn.start();
			}
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public boolean isDown() { return isDown; }
	
	public String getServername() { return servername; }
	
	@Override
	public BaseUser getUser(String username) {
		BaseUser u;
		lock.readLock().lock();
		u = users.get(username);
		lock.readLock().unlock();
		return u;
	}
	
	public ChatGroup getGroup(String groupname) {
		ChatGroup group;
		lock.readLock().lock();
		group = groups.get(groupname);
		lock.readLock().unlock();
		return group;
	}
	
	public Set<String> getGroups() {
		Set<String> groupNames;
		lock.readLock().lock();
		groupNames = this.groups.keySet();
		lock.readLock().unlock();
		return groupNames;
	}
	
	public Set<String> getActiveUsers() {
		Set<String> userNames;
		lock.readLock().lock();
		userNames = users.keySet();
		lock.readLock().unlock();
		return userNames;
	}
	
	public Set<String> getAllUsers() {
		ResultSet usernames;
		Set<String> registeredUsers = new HashSet<String>();
		try {
			usernames = DBHandler.getUsers();
			while(usernames.next()) {
				String s = usernames.getString("username");
				registeredUsers.add(s);
			}
		} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		return registeredUsers;
	}
	
	public int getNumUsers(){
		int num;
		lock.readLock().lock();
		num = users.size();
		lock.readLock().unlock();
		return num;
	}
	
	public int getNumGroups(){
		int num;
		lock.readLock().lock();
		num = groups.size();
		lock.readLock().unlock();
		return num;
	}
	
	public void addServer(String name, ServerConnection conn){
		servers.put(name, conn);
	}
	
	public void removeServer(String name){
		servers.remove(name);
	}
	
	private void initUserGroups(User u){
		ResultSet rs;
		try {
			rs = DBHandler.getUserMemberships(u.getUsername());
			while(rs.next()){
				ChatGroup group = groups.get(rs.getString("gname"));
				group.addLoggedInUser(u.getUsername(), u);
				u.addToGroups(group.getName());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public ServerReply addUser(String username, String password){
		lock.writeLock().lock();
		Set<String> allNames = new HashSet<String>();
		allNames.addAll(onlineNames);
		Set<String> registeredUsers = getAllUsers();
		allNames.addAll(registeredUsers);
		if(allNames.contains(username)) {
			lock.writeLock().unlock();
			return ServerReply.REJECTED;
		}
		SecureRandom random = null;
		byte[] salt = null;
		try {
			random = SecureRandom.getInstance("SHA1PRNG");
			salt = new byte[2];
			random.nextBytes(salt);
		} catch (NoSuchAlgorithmException e1) {
			System.err.println("no PRNG algorithm");
		}
		String hash = hashPassword(password, salt);
		
		try {
			DBHandler.addUser(username, salt, hash);
		} catch(Exception e) {
			lock.writeLock().unlock();
			e.printStackTrace();
			return ServerReply.REJECTED;
		}
		lock.writeLock().unlock();
		return ServerReply.OK;
	}
	
	public void readlog(String username) throws SQLException{
		lock.readLock().lock();
		List<Message> unsentMessages = DBHandler.readAndClearLog(username);
		for (Message message : unsentMessages) {
			users.get(username).acceptMsg(message);
		}
		lock.readLock().unlock();
	}
	
	@Override
	public LoginError login(String username) { return null; }
	
	public LoginError login(String username, String password) {
		lock.writeLock().lock();
		Set<String> registeredUsers = getAllUsers();
		if (isDown || onlineNames.contains(username) || !registeredUsers.contains(username)) {
			if(addUser(username,password) != ServerReply.OK){
				TestChatServer.logUserLoginFailed(username, new Date(), LoginError.USER_REJECTED);
				lock.writeLock().unlock();
				return LoginError.USER_REJECTED;
			}
		}
		LoginError error = loginAttempt(username, password);
		lock.writeLock().unlock();
		return error;
	}
	
	public LoginError loginAttempt(String username, String password) {
		byte[] salt = null;
		try {
			salt = DBHandler.getSalt(username);
			
			String hash = hashPassword(password, salt);
			if (hash == null || !hash.equals(DBHandler.getHashedPassword(username))) {
				TestChatServer.logUserLoginFailed(username, new Date(), LoginError.USER_REJECTED);
				return LoginError.USER_REJECTED;			
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		User newUser = new User(this, username);
		users.put(username, newUser);
		onlineNames.add(username);
		newUser.connected();
		TestChatServer.logUserLogin(username, new Date());
		initUserGroups(newUser);
		return LoginError.USER_ACCEPTED;		
	}
	
	public String hashPassword(String password, byte[] salt) {
		String hashed = null;
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.reset();
			md.update(salt);
		    hashed = DBHandler.byteToBase64(md.digest(password.getBytes("UTF-8")));
		} catch (Exception e) {
			e.printStackTrace();
		}
	    return hashed;
	}

	@Override
	public boolean logoff(String username) {
		lock.writeLock().lock();
		if (!users.containsKey(username)){
			lock.writeLock().unlock();
			return false;
		}
		try {
			ResultSet rs = DBHandler.getUserMemberships(username);
			while(rs.next()) {
				String g = rs.getString("gname");
				ChatGroup c = groups.get(g);
				if(c != null)
					c.removeLoggedInUser(username);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		users.get(username).logoff();
		onlineNames.remove(username);
		users.remove(username);
		
		lock.writeLock().unlock();	
		return true;
	}
	
	public void joinAck(User user, String gname, ServerReply reply) {
		TransportObject toSend = new TransportObject(Command.join,gname,reply);
		user.queueReply(toSend);
	}
	
	public void leaveAck(User user, String gname, ServerReply reply) {
		TransportObject toSend = new TransportObject(Command.leave,gname,reply);
		user.queueReply(toSend);
	}

	public void startNewTimer(SocketParams params) throws IOException {
		List<Handler> task = new ArrayList<Handler>();
		ExecutorService pool = null;
		try {
			task.add(new Handler(params));
			ObjectOutputStream sent = params.getOutputStream();
			pool = Executors.newFixedThreadPool(10);
			List<Future<Handler>> futures = pool.invokeAll(task);
			if (futures.get(0).isCancelled()) {
				TransportObject sendObject = new TransportObject(ServerReply.timeout);
				sent.writeObject(sendObject);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		pool.shutdownNow();
	}
	
	@Override
	public boolean joinGroup(BaseUser baseUser, String groupname) {
		lock.writeLock().lock();
		ChatGroup group;
		User user = (User) baseUser;
		boolean success = false;
		if (!users.keySet().contains(user.getUsername())) {
			lock.writeLock().unlock();
			return false;
		}
		if (groups.containsKey(groupname)) {
			group = groups.get(groupname);
			success = group.joinGroup(user.getUsername(), user);
			if(user.getUserGroups().contains(groupname)){
				joinAck(user,groupname,ServerReply.ALREADY_MEMBER);
				lock.writeLock().unlock();
				return false;
			}
			if (success){
				user.addToGroups(groupname);
				joinAck(user,groupname,ServerReply.OK_JOIN);
				TestChatServer.logUserJoinGroup(groupname, user.getUsername(), new Date());
			} else
				joinAck(user,groupname,ServerReply.FAIL_FULL);
			lock.writeLock().unlock();
			return success;
		}
		else {
			if (onlineNames.contains(groupname)){
				joinAck(user,groupname,ServerReply.BAD_GROUP);
				lock.writeLock().unlock();
				return false;
			}
			group = new ChatGroup(groupname, this);
			groups.put(groupname, group);
			success = group.joinGroup(user.getUsername(), user);
			user.addToGroups(groupname);
			TestChatServer.logUserJoinGroup(groupname, user.getUsername(), new Date());
			if (success)
				joinAck(user,groupname,ServerReply.OK_CREATE);
			lock.writeLock().unlock();
			return success;
		}
	}

	@Override
	public boolean leaveGroup(BaseUser baseUser, String groupname) {
		User user = (User) baseUser;
		lock.writeLock().lock();
		ChatGroup group = groups.get(groupname);
		if (group == null){
			leaveAck(user,groupname,ServerReply.BAD_GROUP);
			lock.writeLock().unlock();
			return false;
		}
		if (group.leaveGroup(user.getUsername())) {
			leaveAck(user,groupname,ServerReply.OK);
			if(group.getAllUsers().size() <= 0) { 
				groups.remove(group.getName()); 
				onlineNames.remove(group.getName());
			}
			user.removeFromGroups(groupname);
			TestChatServer.logUserLeaveGroup(groupname, user.getUsername(), new Date());
			lock.writeLock().unlock();
			return true;
		}
		else {
			leaveAck(user,groupname,ServerReply.NOT_MEMBER);
		}
		lock.writeLock().unlock();
		return false;
	}

	@Override
	public void shutdown() {
		lock.writeLock().lock();
		Set<String> userNames = users.keySet();
		for(String name: userNames){
			users.get(name).logoff();
		}
		users.clear();
		groups.clear();
		isDown = true;
		lock.writeLock().unlock();
	}
	
	public MsgSendError forward(TransportObject toSend, String username){
		List<Object> serverAddresses = null;
		try {
			serverAddresses = DBHandler.getServerAddresses(username, true);
		} catch (SQLException e) {
			e.printStackTrace();
			return MsgSendError.MESSAGE_FAILED;
		}
		if(serverAddresses!=null){
			ServerConnection home = servers.get(serverAddresses.get(4));
			ServerConnection backup = servers.get(serverAddresses.get(5));
			if(home!=null){
				home.acceptMessage(toSend);
			} else if(backup!=null){
				backup.acceptMessage(toSend);
			} else
				return MsgSendError.MESSAGE_FAILED;
			return MsgSendError.MESSAGE_SENT;
		}
		
		return MsgSendError.MESSAGE_FAILED;
	}
	
	//TODO call forward appropriately
	public MsgSendError processMessage(String source, String dest, String msg, int sqn, String timestamp) {	
		Message message = new Message(timestamp, source, dest, msg);
		message.setSQN(sqn);
		lock.readLock().lock();
		Set<String> registeredUsers = getAllUsers();
		if (users.containsKey(source)) {				//Valid destination user
			if(!registeredUsers.contains(dest)) {
				MsgSendError sendError = MsgSendError.INVALID_DEST;
				lock.readLock().unlock();
				return sendError;
			}
			if (users.containsKey(dest)) {
				
				User destUser = users.get(dest);
				destUser.acceptMsg(message);
			} else if (groups.containsKey(dest)) {		//Group destination
				message.setIsFromGroup();
				ChatGroup group = groups.get(dest);
				MsgSendError sendError = group.forwardMessage(message);
				if (sendError == MsgSendError.NOT_IN_GROUP) {
					TestChatServer.logChatServerDropMsg(message.toString(), new Date());
					lock.readLock().unlock();
					return sendError;
				} else if (sendError == MsgSendError.MESSAGE_FAILED){
					lock.readLock().unlock();
					return sendError;
				}
				
			} else {
				// If dest user is not on this server, forward it. 
				//TestChatServer.logChatServerDropMsg(message.toString(), new Date());
				TransportObject toSend = new TransportObject(source, dest ,sqn, msg, timestamp);
				MsgSendError sendError = forward(toSend, source); 
				lock.readLock().unlock();
				return sendError;
			}
			
		} else {
			TestChatServer.logChatServerDropMsg(message.toString(), new Date());
			lock.readLock().unlock();
			return MsgSendError.INVALID_SOURCE;
		}
		
		lock.readLock().unlock();
		return MsgSendError.MESSAGE_SENT;
	}
	
	@Override
	public void run(){
		
		listenForServers.start();
		
		while(!isDown){
			List<Handler> task = new ArrayList<Handler>();
			Socket newSocket;
			try {
				newSocket = mySocket.accept();
				Handler handler = new Handler(newSocket);
				task.add(handler);
				Thread t = new FirstThread(task, handler);
				t.start();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	class FirstThread extends Thread {
		private List<Handler> task;
		private Handler handler;
		
		public FirstThread(List<Handler> task, Handler handler) {
			this.task = task;
			this.handler = handler;
		}
		
		public void run() {
			ExecutorService pool = null;
			try {
				pool = Executors.newFixedThreadPool(10);
				List<Future<Handler>> futures = pool.invokeAll(task);
				if (futures.get(0).isCancelled()) {
					ObjectOutputStream sent = handler.sent;
					TransportObject sendObject = new TransportObject(ServerReply.timeout);
					sent.writeObject(sendObject);
					handler.socket.close();
				}
			} catch (Exception e){
				e.printStackTrace();
			}
			pool.shutdownNow();
		}
	}
	
	class Handler implements Callable<ChatServer.Handler>, Runnable {
		private final Socket socket;
		    Handler(Socket socket) throws IOException { 
		    	this.socket = socket;
		    	received = new ObjectInputStream(socket.getInputStream());
				sent = new ObjectOutputStream(socket.getOutputStream());
		    }
		    
		    Handler(SocketParams params) {
		    	this.socket = params.getMySocket();
		    	received = params.getInputStream();
		    	sent = params.getOutputStream();
		    }
		    private ObjectInputStream received;
			private ObjectOutputStream sent;
		    public void run() {
		    		
		    }
			@Override
			public Handler call() throws Exception {
		    	TransportObject recObject = null;
		    	while(recObject == null) {
					try {
						recObject = (TransportObject) received.readObject();
					} catch (EOFException e) {
						System.err.println("user connection dropped/finished");
						return null;
					} catch (SocketException e) {
						System.err.println("user socket exception");
						return null;
					} catch (Exception e) {
						e.printStackTrace();
						return null;
					}
					if (recObject != null) {
						Command type = recObject.getCommand();
						if (type == Command.login) {
							String username = recObject.getUsername();
							String password = recObject.getPassword();
							LoginError loginError = login(username,password);
							TransportObject sendObject;
							if (loginError == LoginError.USER_ACCEPTED) {
								sendObject = new TransportObject(Command.login, ServerReply.OK);
								User newUser = (User) getUser(username);
								newUser.setSocket(socket, received, sent);
							} else if (loginError == LoginError.USER_DROPPED || loginError == LoginError.USER_REJECTED){
								sendObject = new TransportObject(Command.login, ServerReply.REJECTED);
								recObject = null;
							} else {
								sendObject = new TransportObject(ServerReply.error);
								recObject = null;
							}
							try {
								sent.writeObject(sendObject);
							} catch (IOException e) {
								e.printStackTrace();
							}	
						} else {
							recObject = null;
						}
					}
		    	}
		    	return null;
			}
	}
	
	public double processBenchmarking() {
		return 0;
	}
	
	public static void main(String[] args) throws Exception{
		if (args.length != 6) {
			throw new Exception("Invalid number of args to command");
		}
		if(!"--name".equals(args[0]) || !"--c_port".equals(args[2]) || !"--s_port".equals(args[4]))
			throw new Exception("Invalid parameter args");
		String servername = args[1];
		int clientport = Integer.parseInt(args[3]);
		int serverport = Integer.parseInt(args[5]);
		ChatServer chatServer = new ChatServer(servername,clientport,serverport);
		BufferedReader commands = new BufferedReader(new InputStreamReader(System.in));
		while (!chatServer.isDown()) {
			String line = commands.readLine();
			if(line == null)
				break;
			String[] tokens = line.split(" ");
			if (tokens[0].equals("users")) {
				if(tokens.length==1) // get users
					System.err.println(chatServer.getAllUsers());
				else { // get users from a specific group
					ChatGroup group = chatServer.getGroup(tokens[1]);
					if(group == null)
						System.err.println("no such group: " + tokens[1]);
					else{
						Set<String> userList = group.getAllUsers();
						System.err.println(userList);
					}
				}
			} else if(tokens[0].equals("groups")) {
				System.err.println(chatServer.getGroups());
			} else if (tokens[0].equals("active-users")) {
				if(tokens.length == 1) // get logged in users
					System.err.println(chatServer.getActiveUsers());
				else { // get logged in users from a specific group
					ChatGroup group = chatServer.getGroup(tokens[1]);
					if(group == null)
						System.err.println("no such group: " + tokens[1]);
					else{
						Set<String> userList = group.getAllUsers();
						System.err.println(userList);
					}
				}
			} else if (tokens[0].equals("shutdown")) {
				chatServer.shutdown();
			} else if (tokens[0].equals("thread-count")) {
				System.err.println(Thread.activeCount());
			}
		}
	}
}
