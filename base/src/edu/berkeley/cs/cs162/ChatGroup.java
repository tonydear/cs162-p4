package edu.berkeley.cs.cs162;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ChatGroup {
	private String name;
	private ChatServer myServer;
	
	ChatGroup(String initname, ChatServer cs) {
		name = initname;
		myServer = cs;
	}
	
	public Set<String> getAllUsers() {
		ResultSet rs = DBHandler.getGroupMembers(name);
		Set<String> userList = new HashSet<String>();
		if(rs != null){
			while(rs.next()) {
				userList.add(rs.getString("username"));
			}
		}	
		return userList;
	}
	
	public void addUser(String uname){
		try {
			DBHandler.addToGroup(uname, name);
		} catch (SQLException e) {
			System.err.println("User " + uname + " was not added to group " + name + ".");
		}
	}
	
	public void addLoggedInUser(String uname, User u) {
		//loggedInUsers.put(uname, u);
	}
	
	public void removeLoggedInUser(String uname) {
		//loggedInUsers.remove(uname);
	}
	
	public int getNumUsers() {
		ResultSet rs;
		try {
			rs = DBHandler.getGroupMembers(name);
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(rs != null)
			return rs.getFetchSize();
		else
			return 0;
	}
	
	public String getName() {
		return name;
	}
	
	public boolean onCreate() {
		return true;
	}
	
	public boolean onDelete() {
		return true;
	}
	
	public boolean joinGroup(String user, BaseUser userObj) {
		try {
			DBHandler.addToGroup(user,name);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public boolean leaveGroup(String user) {
		try {
			DBHandler.removeFromGroup(user,name);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public synchronized MsgSendError forwardMessage(Message msg) { // returns SENT even if just stored on db
		//generate list of users from database
		ResultSet rs = DBHandler.getGroupMembers(name);
		Set<String> userList = new HashSet<String>();
		if(rs != null){
			while(rs.next()) {
				userList.add(rs.getString("username"));
			}
		}
		
		//check sender belongs in group
		if (! userList.contains(msg.getSource()))
			return MsgSendError.NOT_IN_GROUP;
		
		Iterator<String> it = userList.iterator();
		boolean success = true;
		MsgSendError returnval = MsgSendError.MESSAGE_SENT;
		while(it.hasNext()) {
			String username = it.next();
			TransportObject toSend = new TransportObject(ServerReply.receive,msg.getSource(),
					msg.getDest(),msg.getContent(),msg.getTimestamp(),msg.getSQN());
			MsgSendError response = myServer.GroupForward(toSend, username);
			if(response == MsgSendError.MESSAGE_FAILED)
				returnval = MsgSendError.MESSAGE_FAILED;
		}
		return returnval;
	}
}
