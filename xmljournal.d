/**
A drop-in replacement for systemd-journald that poops XML logs. Very feature incomplete. Don't use.

Loicense:	WTFPL
CoC:		Don't be a dickwagon. Or do. Whatever; read the loicense.
*/

/*TODO
	Read /dev/kmsg for kernel messages
	Validate XML
	Install signal handler for shutdown
	Add stuff to TODO list
*/

import
	std.file,
	std.stdio,
	std.string,
	std.socket,
	std.algorithm,
	std.process,
	std.datetime;
import std.xml:encode;
import std.conv:to;

// Globals for crutchy
string journal_date;
string journal_root;
string journal_file;

//	I'm posimative I've seen an stdlib function for this but I can't find it now.
auto string2array(String)(String buffer)
{
	string[string] result;

	foreach(row; buffer.splitter("\n"))
	{
		auto y = row.findSplit("=");
		if(y)
			result[y[0].toLower.idup] = y[2].idup;
	}
	return result;
}

auto array2xml(string[string] items)
{
	string result;
	result.reserve(items.length*32);// Pretend to be efficient

	foreach(key, val; items)
	{
		if(val.length)
			result~= "<"~key~">"~val.encode~"</"~key~">\n";
	}
	return result;
}

auto get_hostname()
{
	const hn = "/etc/hostname";
	if(std.file.exists(hn))
		return chomp(cast(string)std.file.read(hn));
	else
		return "";
}
void emit_header()
{
	journal_file.append(`<?xml version="1.0" encoding="UTF-8"?>`~'\n'~`<session id="`~journal_date~"\">\n<host name=\""~get_hostname~"\"/>\n\n");
}

void emit_footer()
{
	journal_file.append("</session>");
}

// It turns out I probably don't need this but I'm not refactoring.
enum State {running = "Running", stopped = "Stopped", starting = "Starting"};

struct Stream_Properties
{
	string name,
	unit;
	byte
		priority,
		level_prefix;

//	We're storing these values but not implementing their related functions
	bool
		forward_to_syslog,
		forward_to_kmsg,
		forward_to_console;
}

// A little shortcut
char[] extract(String)(ref char[] buffer, String delimiter)
{
	auto x = buffer.findSplit(delimiter);
	buffer = x[2];
	return x[0];
}

void write_log(Stream_Properties props, char[] message, State state=State.running)
{
	write_log(props, ["message":message.chomp.idup], state);
}

void write_log(Stream_Properties props, string[string] items, State state=State.running)
{
	auto buffer = "<event>\n<time>"~Clock.currTime(UTC()).toISOExtString~"</time>\n";

// Being a little lazy here. These are only set for the stdin socket and systemd will send these values in its messages from the main journaling socket.
	if(props.name)
		buffer~="<name>"~props.name.encode~"</name>\n";
	if(props.unit)
		buffer~="<unit>"~props.unit.encode~"</unit>\n";
	if(state!= State.running)
		buffer~="<state>"~state~"</state>\n";

	buffer~=items.array2xml~"</event>\n\n";
	journal_file.append(buffer);
}

auto rx(Socket sockety)
{
	char[1024] miniBuff;
	char[] buffer;
	long rxed;
	do
	{
		rxed = sockety.receive(miniBuff);
		buffer~= miniBuff[0..rxed];
	} while (rxed == 1024);

	if(rxed == 0)
		throw new Exception("Connection closed");

	return buffer;
}

void notify(string[] lines)
{
	auto notifySock = new Socket(AddressFamily.UNIX, SocketType.DGRAM);
	notifySock.sendTo(lines.join("\n"), new UnixAddress("/run/systemd/notify"));
}

void be_a_server()
{
	Stream_Properties[Socket] sockets;
	journal_date = Clock.currTime(UTC()).toISOString;
	journal_root = "/var/log/";
	journal_file = journal_root~"journal-"~journal_date~".xml";

	auto last_watchdog = Clock.currTime;

	emit_header();
	if(std.file.exists(journal_root~"journal-current.xml"))
		std.file.remove(journal_root~"journal-current.xml");
	symlink("journal-"~journal_date~".xml", journal_root~"journal-current.xml");

// Figure out the order of the passed-in sockets. Since devlog and socket are defined in the same unit file, I'll assume they're always in the order they're defined. Am I an enterprise dev yet?
	ulong devlog;
	ulong stdout;
	ulong socket;
	auto x = environment.get("LISTEN_FDNAMES").split(":");
	if(x[0] == "systemd-journald-dev-log.socket")
	{
		devlog=3;
		stdout=4;
		socket=5;
	}
	else
	{
		devlog=5;
		stdout=3;
		socket=4;
	}

//	Most stuff comes in here, through systemd's API
	auto mainSock = new Socket(cast(socket_t)socket, AddressFamily.UNIX); /// /run/systemd/journal/socket (Datagram)

//	stdout (and stderr I guess) of daemons comes in here
	auto stdOutSock = new Socket(cast(socket_t)stdout, AddressFamily.UNIX); /// /run/systemd/journal/stdout (Stream)

//	I don't have anything using good ol' syslog output to test this
	auto devlogSock = new Socket(cast(socket_t)devlog, AddressFamily.UNIX); /// /run/systemd/journal/dev-log (Datagram)

	auto socketSet = new SocketSet;

	notify(["READY=1", "STATUS=Processing requests."]);

//	This was easier than refactoring
	Stream_Properties sp = {};

	auto running = true;
	while(running)
	{
		socketSet.add(mainSock);
		socketSet.add(stdOutSock);
		socketSet.add(devlogSock);

		foreach(s; sockets.keys)
			socketSet.add(s);

		if(Socket.select(socketSet, null, null, dur!"seconds"(10)) > 0)
		{
			foreach(sock, props; sockets)
			{
				char[] y;
				try{
					if(socketSet.isSet(sock))
					{
						y = sock.rx;
						if(y.length)
							props.write_log(y);
					}
				}
				catch(Exception e)
				{
					props.write_log(y, State.stopped);
					sockets.remove(sock);
					socketSet.remove(sock);
				}
			}
			if(socketSet.isSet(mainSock))
			{
				char[] y;
				try{
					y = mainSock.rx;
					if(y)
					sp.write_log(y.string2array);
				}
				catch(Exception e)
				{
					sp.write_log(y, State.stopped);
					running = false;
				}
			}
			if(socketSet.isSet(stdOutSock))
			{
				auto xSocket = stdOutSock.accept;
				if(xSocket.handle > 0){
				//	Read in the header and store it
					auto y 				= xSocket.rx;
					Stream_Properties z;
					z.name				= y.extract("\n").idup;
					z.unit				= y.extract("\n").idup;
					z.priority			= y.extract("\n").to!byte;
					z.level_prefix		= y.extract("\n").to!byte;
					z.forward_to_syslog	= y.extract("\n")=="1";
					z.forward_to_kmsg	= y.extract("\n")=="1";
					z.forward_to_console= y.extract("\n")=="1";

					z.write_log(y, State.starting);
					sockets[xSocket] 	= z;
				}
			}
			if(socketSet.isSet(devlogSock))
			{
				char[] y;
				try{
					y = devlogSock.rx;
					if(y)
						sp.write_log(y, State.starting);
				}
				catch(Exception e)
				{
					sp.write_log(y, State.stopped);
				}
			}
		}
		auto current_time = Clock.currTime;
//		The ping interval is specified in systemd-journald.service but professionals hard-code their values.
		if(current_time - last_watchdog > dur!"minutes"(2))
		{
			notify(["WATCHDOG=1"]);
			last_watchdog = current_time;
		}
	}
	
// This doesn't happen, of course. Probably should install a signal handler to detect shutdown.
	emit_footer();
}

int main(string[] params)
{
	try{
		be_a_server();}
	catch(Exception e)
	{
		toFile(e.info.toString~"\n\n"~e.file~" ["~e.line.to!string~"] "~e.msg, "/var/log/journal.exception");
		return -1;
	}
	return 0
	;
}

unittest
{
	char[] x = "This here's a test:string.".dup;
	assert(x.extract(" ") == "This");
	assert(x.extract("'") == "here");
	assert(x.extract(":") == "s a test");
	assert(x.extract(".") == "string");
	assert(x.extract("") == "");
}

//TODO This one will fail if your array happens to be in a different order. Do some sorting.
unittest
{
	auto x = ["a":"zero", "b":"one","c":"two"];
	assert(x.array2xml ==
`<c>two</c>
<a>zero</a>
<b>one</b>
`);
}
