package edu.berkeley.cs.cs162;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import sun.misc.*;


public class DBHandler {
    private static Connection conn;
    static {   	
        conn = null;
        Properties connectionProps = new Properties();
        connectionProps.put("user", "group14");
        connectionProps.put("password", "fuck you");
        try {
            conn = DriverManager.
                getConnection("jdbc:" + "mysql" + "://" + "ec2-50-17-180-71.compute-1.amazonaws.com" +
                              ":" + 3306 + "/" + "group14", connectionProps);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public static void addUser(String username, byte[] salt, String hashedPassword) throws SQLException {
    	PreparedStatement pstmt = conn.prepareStatement("INSERT INTO Users (username, salt, encrypted_password) VALUES (?,?,?)");
    	if(pstmt == null) return;
    	pstmt.setString(1, username);
    	pstmt.setString(2, byteToBase64(salt));
    	
    	pstmt.setString(3, hashedPassword);
    	pstmt.executeUpdate();
    }
    
    public static void addToGroup(String uname, String gname) throws SQLException {
    	PreparedStatement pstmt = null;
    	try {
    		pstmt = conn.prepareStatement("INSERT INTO Memberships (gname, username)" +
    		" VALUES (?,?)");
    		pstmt.setString(1, gname);
    		pstmt.setString(2, uname);
    		pstmt.executeUpdate();
    	} 
    	finally {
    		if(pstmt!=null) pstmt.close();
    	}
    }
    
    public static void writeLog(Message msg, String recipient) throws SQLException{
    	PreparedStatement pstmt = null;
    	pstmt = conn.prepareStatement("INSERT INTO Messages (sender, sqn, timestamp, destination, message, recipient) " + 
    			"VALUES (?,?,?,?,?,?)");
    	pstmt.setString(1, msg.getSource());
    	pstmt.setInt(2, msg.getSQN());
    	long time = (long) Double.parseDouble(msg.getTimestamp());
    	pstmt.setTime(3,new Time(time));
    	pstmt.setString(4, msg.getDest());
    	pstmt.setString(5, msg.getContent());
    	pstmt.setString(6, recipient);
    	pstmt.executeUpdate();
    }
    
    public static void removeFromGroup(String uname, String gname) throws SQLException{
    	PreparedStatement pstmt = null;
    	try {
    		pstmt = conn.prepareStatement("DELETE FROM Memberships WHERE gname = ? AND username = ?");
    		pstmt.setString(1, gname);
    		pstmt.setString(2, uname);
    		pstmt.executeUpdate();
    	} 
    	finally {
    		if(pstmt!=null) pstmt.close();
    	}
    }
    
    public static List<Message> readAndClearLog(String uname) throws SQLException{
    	List<Message> messages = new ArrayList<Message>();
    	PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM Messages WHERE recipient = ?");
    	if(pstmt == null) return null;
    	pstmt.setString(1, uname);
    	ResultSet rs = pstmt.executeQuery();
    	while(rs.next()){
    		String sender = rs.getString("sender");
    		int sqn = rs.getInt("sqn");
    		Long timestamp = rs.getTime("timestamp").getTime();
    		String destination = rs.getString("destination");
    		String content = rs.getString("message");
    		Message msg = new Message(timestamp.toString(),sender,destination,content);
    		msg.setSQN(sqn);
    		messages.add(msg);
    	}
    	pstmt = conn.prepareStatement("DELETE FROM Messages WHERE recipient = ?");
    	pstmt.setString(1, uname);
    	pstmt.executeUpdate();
    	return messages;
    }
    
    /**
     * From a base 64 representation, returns the corresponding byte[] 
     * @param data String The base64 representation
     * @return byte[]
     * @throws IOException
     */
    public static byte[] base64ToByte(String data) throws IOException {
        BASE64Decoder decoder = new BASE64Decoder();
        return decoder.decodeBuffer(data);
    }
  
    /**
     * From a byte[] returns a base 64 representation
     * @param data byte[]
     * @return String
     * @throws IOException
     */
    public static String byteToBase64(byte[] data){
        BASE64Encoder endecoder = new BASE64Encoder();
        return endecoder.encode(data);
    }
    
    public static byte[] getSalt(String username) throws SQLException, IOException {
    	PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM Users WHERE username = ?");
    	if (pstmt == null) return null;
    	pstmt.setString(1, username);
    	ResultSet rs = pstmt.executeQuery();
    	rs.next();
    	return base64ToByte(rs.getString("salt"));
    	
    }
    
    public static String getHashedPassword(String uname) throws SQLException {
    	PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM Users WHERE username = ?");
    	if (pstmt == null) return null;
    	pstmt.setString(1, uname);
    	ResultSet rs = pstmt.executeQuery();
    	rs.next();
    	return rs.getString("encrypted_password");
    }
    
    public static ResultSet getUsers() throws SQLException {
    	Statement stmt = conn.createStatement();
    	return stmt.executeQuery("SELECT username FROM Users");
    }
    
    public static ResultSet getGroups() throws SQLException {
    	Statement stmt = conn.createStatement();
    	return stmt.executeQuery("SELECT DISTINCT(gname) FROM Memberships");
    }
    
    public static ResultSet getMemberships() throws SQLException {
    	Statement stmt = conn.createStatement();
    	return stmt.executeQuery("SELECT * FROM Memberships");
    }
    
    public static ResultSet getServers() throws SQLException {
    	Statement stmt = conn.createStatement();
    	return stmt.executeQuery("SELECT name, host FROM server_info");
    }
    
    public static ResultSet getUserMemberships(String username) throws SQLException {
    	PreparedStatement pstmt = conn.prepareStatement("SELECT gname FROM Memberships WHERE username = ?");
    	pstmt.setString(1, username);
    	return pstmt.executeQuery();
    }
    
    public static ResultSet getGroupMembers(String gname) throws SQLException {
    	PreparedStatement pstmt = conn.prepareStatement("SELECT username FROM Memberships WHERE gname = ?");
    	pstmt.setString(1, gname);
    	return pstmt.executeQuery();
    }
    
    public static String getIP(String name) throws SQLException {
    	PreparedStatement pstmt = conn.prepareStatement("SELECT host FROM server_info WHERE name = ?");
    	pstmt.setString(1, name);
    	ResultSet rs = pstmt.executeQuery();
    	rs.next();
    	return rs.getString("host");     	
    }
    
    public static int getServerPort(String name) throws SQLException {
    	PreparedStatement pstmt = conn.prepareStatement("SELECT s_port FROM server_info WHERE name = ?");
    	pstmt.setString(1, name);
    	ResultSet rs = pstmt.executeQuery();
    	rs.next();
    	return rs.getInt("s_port");  
    }
    
    public static int getClientPort(String name) throws SQLException {
    	PreparedStatement pstmt = conn.prepareStatement("SELECT c_port FROM server_info WHERE name = ?");
    	pstmt.setString(1, name);
    	ResultSet rs = pstmt.executeQuery();
    	rs.next();
    	return rs.getInt("c_port");   	
    }
    
    public static List<String> getServerNames(String username) throws SQLException {
    	//Query for all server names
    	HashMap<BigInteger, String> serverHashes = new HashMap<BigInteger, String>();
    	PreparedStatement allServers = conn.prepareStatement("SELECT name FROM server_info");
    	ResultSet rs = allServers.executeQuery();
    	
    	//Hash all server names into serverHashes<hash, name>
    	while (rs.next()) {
			try {
				String server = rs.getString("name");
				MessageDigest digest = MessageDigest.getInstance("SHA-256");
	            digest.update(server.getBytes());
	            byte[] hash = digest.digest();
	            serverHashes.put(new BigInteger(hash), server);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
    	}
    	
    	//Create list of hashes only and sort
    	List<BigInteger> hashes = new ArrayList<BigInteger>(serverHashes.keySet());
    	Collections.sort(hashes);
    	
    	//Hash username into userHash
    	BigInteger userHash = null;
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(username.getBytes());
            byte[] hash = digest.digest();
            userHash = new BigInteger(hash);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
    	
		//Try to find the two next larger hashes than userHash
		BigInteger first = null;
		BigInteger second = null;
		for (BigInteger hash : hashes) {
			if (first == null && hash.compareTo(userHash) >= 0)
				first = hash;
			else if (first != null) {
				second = hash;
				break;
			}
		}
		
		//If one or both were not found, then must wrap around to the beginning
		if (first == null) {
			first = hashes.get(0);
			second = hashes.get(1);
		} else if (second == null)
			second = hashes.get(0);
		
		//Get server names corresponding to the hashes
		List<String> addresses = new ArrayList<String>();
		addresses.add(serverHashes.get(first));
		addresses.add(serverHashes.get(second));
		
		return addresses;
    }
    
    public static void addRTT(double rtt, String username) throws SQLException {
    	PreparedStatement pstmt = conn.prepareStatement("INSERT INTO Rtt (rtt, username, timestamp) VALUES (?,?,?)");
    	long time = System.currentTimeMillis();
    	pstmt.setDouble(1, rtt);
    	pstmt.setString(2, username);
    	pstmt.setTime(3, new Time(time));
    	pstmt.executeUpdate();
    }
}