package com.github.gaoyangthu.ftp;

import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.System.currentTimeMillis;
import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;

/**
 * Netty handler, partial implementation of <a href="http://tools.ietf.org/html/rfc959">RFC 959 "File Transfer Protocol (FTP)"</a>
 * for receiving FTP files. Both active and passive modes are supported.
 *
 * Created by IntelliJ IDEA
 * Project: ftpService
 * Author: GaoYang
 * Date: 2014/6/17 0017
 */
public class FtpServerHandler extends SimpleChannelUpstreamHandler {
	private static final Logger logger = LoggerFactory.getLogger(FtpServerHandler.class);
	private static final byte CR = 13;
	private static final byte LF = 10;
	protected final byte[] CRLF = new byte[]{CR, LF};
	private static final Charset ASCII = Charset.forName("US-ASCII");

	private final DataReceiver receiver;
	private final byte[] passiveAddress;
	private final byte[] passiveAdvertisedAddress;
	private final int lowestPassivePort;
	private final int highestPassivePort;
	private final int passiveOpenAttempts;
	private final int passiveSoTimeoutMillis;

	// netty may be configured to use different worker threads with single handler
	// even if handler is created for each pipeline
	protected AtomicReference<String> hdfsBaseDir = new AtomicReference<String>("/tmp/ftp");
	protected AtomicReference<String> hdfsDir = new AtomicReference<String>("/tmp/ftp");
	protected AtomicReference<String> curDir = new AtomicReference<String>("/");
	protected AtomicReference<String> lastCommand = new AtomicReference<String>("");
	protected AtomicReference<Socket> activeSocket = new AtomicReference<Socket>();
	protected AtomicReference<ServerSocket> passiveSocket = new AtomicReference<ServerSocket>();

	/**
	 * Constructor for FTP active mode
	 *
	 * @param receiver data receiver implementation
	 */
	public FtpServerHandler(DataReceiver receiver) {
		this(receiver, new byte[]{127, 0, 0, 1}, new byte[]{127, 0, 0, 1}, 2121, 4242, 10, 0);
	}

	/**
	 * Constructor for FTP passive mode
	 *
	 * @param receiver data receiver implementation
	 * @param passiveAddress passive IP address that will be used for server socket bind
	 * @param passiveAdvertisedAddress passive IP address that will be advertized to client
	 * @param lowestPassivePort lowest bound of passive ports range
	 * @param highestPassivePort highest bound of passive ports range
	 * @param passiveOpenAttempts number of ports to choose for passive socket open before reporting error
	 */
	public FtpServerHandler(DataReceiver receiver, InetAddress passiveAddress, InetAddress passiveAdvertisedAddress,
	                        int lowestPassivePort, int highestPassivePort, int passiveOpenAttempts, int passiveSoTimeoutMillis) {
		this(receiver, passiveAddress.getAddress(), passiveAdvertisedAddress.getAddress(), lowestPassivePort,
			highestPassivePort, passiveOpenAttempts, passiveSoTimeoutMillis);
	}

	/**
	 * Constructor for FTP passive mode with separate advertized address
	 *
	 * @param receiver data receiver implementation
	 * @param passiveAddress passive IP address that will be used for server socket bind
	 * @param passiveAdvertisedAddress passive IP address that will be advertized to client
	 * @param lowestPassivePort lowest bound of passive ports range
	 * @param highestPassivePort highest bound of passive ports range
	 * @param passiveOpenAttempts number of ports to choose for passive socket open before reporting error
	 */
	public FtpServerHandler(DataReceiver receiver, byte[] passiveAddress, byte[] passiveAdvertisedAddress,
	                        int lowestPassivePort, int highestPassivePort, int passiveOpenAttempts, int passiveSoTimeoutMillis) {
		if(null == receiver) throw new IllegalArgumentException("Provided receiver is null");
		this.receiver = receiver;
		if(null == passiveAddress) throw new IllegalArgumentException("Provided passiveAddress is null");
		this.passiveAddress = passiveAddress;
		this.passiveAdvertisedAddress = passiveAdvertisedAddress;
		if (lowestPassivePort <= 0 || lowestPassivePort >= 1 << 16) throw new IllegalArgumentException(
			"Provided lowestPassivePort: [" + lowestPassivePort + "] ia out of valid range");
		if (highestPassivePort <= 0 || highestPassivePort >= 1 << 16) throw new IllegalArgumentException(
			"Provided highestPassivePort: [" + highestPassivePort + "] ia out of valid range");
		if (lowestPassivePort > highestPassivePort) throw new IllegalArgumentException(
			"Provided lowestPassivePort: [" + lowestPassivePort + "] must be not greater than " +
				"highestPassivePort: [" + highestPassivePort + "]");
		this.lowestPassivePort = lowestPassivePort;
		this.highestPassivePort = highestPassivePort;
		if(passiveOpenAttempts <= 0) throw new IllegalArgumentException(
			"Provided passiveOpenAttempts: [" + passiveOpenAttempts + "] must be positive");
		this.passiveOpenAttempts = passiveOpenAttempts;
		if(passiveSoTimeoutMillis < 0) throw new IllegalArgumentException(
			"Provided passiveSoTimeoutMillis: [" + passiveSoTimeoutMillis + "] must be positive");
		this.passiveSoTimeoutMillis = passiveSoTimeoutMillis;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		String message = ((String) e.getMessage()).trim();
		if (message.length() < 3) send("501 Syntax error", ctx, message, "");
		String cmd = 3 == message.length() ? message.substring(0, 3) : message.substring(0, 4).trim();
		String args = message.length() > cmd.length() ? message.substring(cmd.length() + 1) : "";
		// dispatch
		if ("USER".equals(cmd)) send("230 USER LOGGED IN", ctx, cmd, args);
		//else if ("CWD".equals(cmd)) { hdfsDir.set(hdfsBaseDir.get()+args); send("250 CWD command successful", ctx, cmd, args); }
		else if ("CWD".equals(cmd)) cwd(ctx, args);
		else if ("PWD".equals(cmd)) send("257 \"" + hdfsDir.get() + "\" is current directory", ctx, cmd, args);
		else if ("MKD".equals(cmd)) send("521 \"" + args + "\" directory exists", ctx, cmd, args);
		else if ("DELE".equals(cmd)) send("550 " + args + ": no such file or directory", ctx, cmd, args);
		else if ("RMD".equals(cmd)) send("550 " + args + ": no such file or directory", ctx, cmd, args);
		else if ("RNFR".equals(cmd)) send("350 File exists, ready for destination name", ctx, cmd, args);
		else if ("RNTO".equals(cmd)) send("250 RNTO command successful", ctx, cmd, args);
		else if ("SYST".equals(cmd)) send("215 UNIX Type: Java custom implementation", ctx, cmd, args);
		else if ("NOOP".equals(cmd)) send("200 OK", ctx, cmd, args);
		else if ("TYPE".equals(cmd)) type(ctx, args);
		else if ("PORT".equals(cmd)) port(ctx, args);
		else if ("PASV".equals(cmd)) pasv(ctx, args);
		else if ("LIST".equals(cmd)) list(ctx, args);
		else if ("STOR".equals(cmd)) stor(ctx, args);
		else if ("QUIT".equals(cmd)) send("221 QUIT command successful", ctx, "QUIT", args);
		else if ("ALLO".equals(cmd)) send("202 No storage allocation necessary", ctx, "ALLO", args);
		else if ("RETR".equals(cmd)) retr(ctx, args);
		else send("500 Command unrecognized", ctx, cmd, args);
		lastCommand.set(cmd);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		logger.warn("Exception caught in FtpServerHandler", e.getCause());
		if(ctx.getChannel().isOpen()) {
			send("500 Unspecified error", ctx, String.valueOf(e.getCause().getMessage()), "");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		send("220 Service ready", ctx, "[connected]", "");
	}

	/**
	 * TYPE command handler
	 *
	 * @param ctx handler context
	 * @param args command arguments
	 */
	protected void type(ChannelHandlerContext ctx, String args) {
		if ("I".equals(args)) send("200 Type set to IMAGE NONPRINT", ctx, "TYPE", args);
		else if ("A".equals(args)) send("200 Type set to ASCII NONPRINT", ctx, "TYPE", args);
		else send("504 Command not implemented for that parameter", ctx, "TYPE", args);
	}

	/**
	 * PORT command handler
	 *
	 * @param ctx handler context
	 * @param args command arguments
	 */
	protected void port(ChannelHandlerContext ctx, String args) {
		InetSocketAddress addr = parsePortArgs(args);
		Socket as = activeSocket.get();
		if (logger.isTraceEnabled()) logger.trace(String.valueOf(addr));
		if (null == addr) send("501 Syntax error in parameters or arguments", ctx, "PORT", args);
		else if (null != as) send("503 Bad sequence of commands", ctx, "PORT", args);
		else {
			try {
				Socket created = new Socket(addr.getAddress(), addr.getPort());
				boolean success = activeSocket.compareAndSet(null, created);
				if(success) send("200 PORT command successful", ctx, "PORT", args);
				else {
					logger.warn("Invalid concurrent handler usage detected");
					send("425 Server error", ctx, "PORT", args);
				}
			} catch (IOException e1) {
				logger.warn("Exception thrown on opening active socket to address: [" + addr + "]", e1);
				closeActiveSocket(null);
				send("552 Requested file action aborted", ctx, "PORT", args);
			}
		}
	}

	/**
	 * PASV command handler
	 *
	 * @param ctx handler context
	 * @param args command arguments
	 * @throws InterruptedException
	 */
	protected void pasv(ChannelHandlerContext ctx, String args) throws InterruptedException {
		ServerSocket ps = passiveSocket.get();
		if(null != ps) {
			logger.warn("Invalid concurrent handler usage detected");
			send("425 Server error", ctx, "PASV", args);
			return;
		}
		for (int i = 0; i < passiveOpenAttempts; i++) {
			int port = choosePassivePort(lowestPassivePort, highestPassivePort);
			int part1 = (byte) (port >> 8) & 0xff;
			int part2 = (byte) (port >> 0) & 0xff;
			InetAddress addr = null;
			try {
				addr = InetAddress.getByAddress(passiveAddress);
				ps = new ServerSocket(port, 0, addr);
				ps.setSoTimeout(passiveSoTimeoutMillis);
				send(String.format("227 Entering Passive Mode (%d,%d,%d,%d,%d,%d)",
					passiveAdvertisedAddress[0] & 0xff, passiveAdvertisedAddress[1] & 0xff,
					passiveAdvertisedAddress[2] & 0xff, passiveAdvertisedAddress[3] & 0xff,
					part1, part2), ctx, "PASV", args);
				break;
			} catch (IOException e1) {
				logger.warn("Exception thrown on binding passive socket to address: [" + addr + "], port: [" + port + "], " +
					"attempt: [" + i + 1 + "] of: [" + passiveOpenAttempts + "]", e1);
				closePassiveSocket(ps);
				// ensure port change
				Thread.sleep(1);
			}
		}
		if(null != ps) {
			boolean success = passiveSocket.compareAndSet(null, ps);
			if(!success) {
				logger.warn("Invalid concurrent handler usage detected");
				send("425 Server error", ctx, "PASV", args);
			}
		}
		else send("551 Requested action aborted", ctx, "PASV", args);
	}

	/**
	 * LIST command handler
	 *
	 * @param ctx handler context
	 * @param args command arguments
	 */
	protected void list(ChannelHandlerContext ctx, String args) {
		if ("PORT".equals(lastCommand.get())) {
			Socket as = activeSocket.get();
			if (null != as) {
				send("150 Opening binary mode data connection for LIST " + args, ctx, "LIST", args);
				try {
					as.getOutputStream().write(CRLF);
					send("226 Transfer complete for LIST", ctx, "", args);
				} catch (IOException e1) {
					logger.warn("Exception thrown on writing through active socket: [" + as + "]", e1);
					send("552 Requested file action aborted", ctx, "LIST", args);
				} finally {
					closeActiveSocket(as);
				}
			} else send("503 Bad sequence of commands", ctx, "LIST", args);
		} else if ("PASV".equals(lastCommand.get())) {
			ServerSocket ps = this.passiveSocket.get();
			if (null != ps) {
				send("150 Opening binary mode data connection for LIST on port: " + ps, ctx, "LIST", args);
				Socket clientSocket = null;
				try {
					clientSocket = ps.accept();
					clientSocket.getOutputStream().write(CRLF);
					clientSocket.getOutputStream().close();
					send("226 Transfer complete for LIST", ctx, "", args);
				} catch (IOException e1) {
					logger.warn("Exception thrown on writing through passive socket: [" + ps + "]," +
						"accepted client socket: [" + clientSocket + "]", e1);
					send("552 Requested file action aborted", ctx, "LIST", args);
				} finally {
					closePassiveSocket(ps);
				}
			} else send("503 Bad sequence of commands", ctx, "LIST", args);
		} else send("503 Bad sequence of commands", ctx, "LIST", args);
	}

	/**
	 * STOP command handler
	 *
	 * @param ctx handler context
	 * @param args command arguments
	 */
	protected void stor(ChannelHandlerContext ctx, String args) {
		if ("PORT".equals(lastCommand.get())) {
			Socket as = activeSocket.get();
			if (null != as) {
				send("150 Opening binary mode data connection for " + args, ctx, "", args);
				try {
					receiver.receive(hdfsDir.get(), args, as.getInputStream());
					send("226 Transfer complete for STOR " + args, ctx, "", args);
				} catch (IOException e1) {
					logger.warn("Exception thrown on reading through active socket: [" + as + "]", e1);
					send("552 Requested file action aborted", ctx, "STOR", args);
				} finally {
					closeActiveSocket(as);
				}
			} else send("503 Bad sequence of commands", ctx, "STOR", args);
		} else if ("PASV".equals(lastCommand.get())) {
			ServerSocket ps = this.passiveSocket.get();
			if (null != ps) {
				send("150 Opening binary mode data connection for " + args, ctx, "STOR", args);
				Socket clientSocket = null;
				try {
					clientSocket = ps.accept();
					receiver.receive(hdfsDir.get(), args, clientSocket.getInputStream());
					send("226 Transfer complete for STOR " + args, ctx, "", args);
				} catch (IOException e1) {
					logger.warn("Exception thrown on reading through passive socket: [" + ps + "], " +
						"accepted client socket: [" + clientSocket + "]", e1);
					send("552 Requested file action aborted", ctx, "STOR", args);
				} finally {
					closePassiveSocket(ps);
				}
			} else send("503 Bad sequence of commands", ctx, "STOR", args);
		} else send("503 Bad sequence of commands", ctx, "STOR", args);
	}

	/**
	 * CWD command handler
	 *
	 * Author: GaoYang
	 * Date: 2014/6/18
	 *
	 * @param ctx handler context
	 * @param args command arguments
	 */
	protected void cwd(ChannelHandlerContext ctx, String args) {
		if (args == null || args.length() == 0 || args.charAt(0) == '/') {
			hdfsDir.set(hdfsBaseDir.get());
		} else if (args.startsWith(".") && args.length() > 2) {
			hdfsDir.set(hdfsBaseDir.get());
		} else if (args.equals("..")) {
			String[] dirs = hdfsDir.get().split("/");
			if (hdfsDir.get().endsWith("/")) {
				if (dirs.length < 6) {
					hdfsDir.set(hdfsBaseDir.get());
				} else {
					StringBuilder sb = new StringBuilder();
					for (int i = 3; i < dirs.length - 1; i++) {
						sb.append("/").append(dirs[i]);
					}
					hdfsDir.set(hdfsBaseDir.get() + sb.toString());
				}
			} else {
				if (dirs.length < 5) {
					hdfsDir.set(hdfsBaseDir.get());
				} else {
					StringBuilder sb = new StringBuilder();
					for (int i = 3; i < dirs.length - 1; i++) {
						sb.append("/").append(dirs[i]);
					}
					hdfsDir.set(hdfsBaseDir.get() + sb.toString());
				}
			}
		} else {
			if (hdfsDir.get().endsWith("/")) {
				hdfsDir.set(hdfsDir.get() + args);
			} else {
				hdfsDir.set(hdfsDir.get() + "/" + args);
			}
		}
		send("250 CWD command successful", ctx, "CWD", args);
	}

	/**
	 * CWD command handler
	 *
	 * Author: GaoYang
	 * Date: 2014/6/18
	 *
	 * @param ctx handler context
	 * @param args command arguments
	 */
	protected void retr(ChannelHandlerContext ctx, String args) {
		send("226 RETR command successful", ctx, "RETR", args);
	}

	protected void send(String response, ChannelHandlerContext ctx, String command, String args) {
		if(logger.isDebugEnabled()) {
			if(command.length() > 0) logger.debug("-> " + command + " " + args);
			logger.debug("<- " + response);
		}
		String line = response + "\r\n";
		byte[] data = line.getBytes(ASCII);
		ctx.getChannel().write(wrappedBuffer(data));
	}

	private static InetSocketAddress parsePortArgs(String portArgs) {
		String[] strParts = portArgs.split(",");
		if (strParts.length != 6) return null;
		byte[] address = new byte[4];
		int[] parts = new int[6];
		for (int i = 0; i < 6; i++) {
			try { parts[i] = Integer.parseInt(strParts[i]); }
			catch (NumberFormatException e) { return null; }
			if (parts[i] < 0 || parts[i] > 255) return null;
		}
		for (int i = 0; i < 4; i++) address[i] = (byte) parts[i];
		int port = parts[4] << 8 | parts[5];
		InetAddress inetAddress;
		try { inetAddress = InetAddress.getByAddress(address); }
		catch (UnknownHostException e) { return null; }
		return new InetSocketAddress(inetAddress, port);
	}

	private static int choosePassivePort(int low, int high) {
		int length = high - low;
		int offset = (int) (currentTimeMillis() % length);
		return low + offset;
	}

	protected void closeActiveSocket(Socket socket) {
		Socket as = null != socket ? socket : activeSocket.get();
		if(null == as) return;
		try {
			as.close();
		} catch (Exception e) {
			logger.warn("Exception thrown on closing active socket", e);
		} finally {
			boolean success = activeSocket.compareAndSet(as, null);
			if(!success) logger.warn("Invalid concurrent handler usage detected");
		}
	}

	protected void closePassiveSocket(ServerSocket socket) {
		ServerSocket ps = null != socket ? socket: passiveSocket.get();
		if(null == ps) return;
		try {
			ps.close();
		} catch (Exception e) {
			logger.warn("Exception thrown on closing server socket", e);
		} finally {
			boolean success = passiveSocket.compareAndSet(ps, null);
			if(!success) logger.warn("Invalid concurrent handler usage detected");
		}
	}
}
