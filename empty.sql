DROP TABLE Users;
DROP TABLE Memberships;
DROP TABLE Messages;
DROP TABLE 'server info';

CREATE TABLE Users (
	username VARCHAR(64) NOT NULL,
	salt VARCHAR(128) NOT NULL,
	encrypted_password VARCHAR(256) NOT NULL,
	PRIMARY KEY (username)
);

CREATE TABLE Memberships (
	username VARCHAR(64) NOT NULL,
	gname VARCHAR(64) NOT NULL,
	PRIMARY KEY (username, gname),
	FOREIGN KEY (username) REFERENCES Users (username)
		ON DELETE CASCADE
);

CREATE TABLE Messages (
	recipient VARCHAR(64) NOT NULL,
	sender VARCHAR(64) NOT NULL,
	sqn INT NOT NULL,
	timestamp TIME NOT NULL,
	destination VARCHAR(64),
	message VARCHAR(1024),
	PRIMARY KEY (recipient, sender, sqn),
	FOREIGN KEY (recipient) REFERENCES Users (username)
		ON DELETE CASCADE,
	FOREIGN KEY (sender) REFERENCES Users (username)
		ON DELETE NO ACTION
);

CREATE TABLE server_ports (
	name varchar(20) NOT NULL,
	server_port int(5),
	client_port int(5),
	PRIMARY KEY (name),
	FOREIGN KEY (name) REFERENCES server_info (name)
		ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS server_info (
	id int(11) NOT NULL AUTO_INCREMENT,
	name varchar(20) NOT NULL,
	host varchar(255) NOT NULL,
	PRIMARY KEY (id)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;
