package poke.monitor;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessage;

import poke.demo.Monitor.HeartPrintListener;
import poke.monitor.HeartMonitor;
import poke.monitor.MonitorHandler;
import poke.monitor.MonitorInitializer;
import poke.monitor.MonitorListener;
import poke.monitor.HeartMonitor.MonitorClosedListener;
import poke.server.management.ManagementInitializer;
import eye.Comm.LeaderElection;
import eye.Comm.Management;
import eye.Comm.Network;
import eye.Comm.LeaderElection.VoteAction;
import eye.Comm.Network.NetworkAction;

public class LeaderElectionMonitor {


	protected static Logger logger = LoggerFactory.getLogger("mgmt");

	protected ChannelFuture channel; // do not use directly, call connect()!
	private EventLoopGroup group;

	private static int N = 0;
	private String whoami;
	private String host;
	private int port;

	// this list is only used if the connection cannot be established - it holds
	// the listeners to be added.
	private List<MonitorListener> listeners = new ArrayList<MonitorListener>();

	private LeaderElectionHandler handler;

	/**
	 * Create a heartbeat message processor.
	 * 
	 * @param host
	 *            the hostname
	 * @param port
	 *            This is the management port
	 */
	public  LeaderElectionMonitor(String whoami, String host, int port) {
		this.whoami = whoami;
		this.host = host;
		this.port = port;
		this.group = new NioEventLoopGroup();
	}

	public LeaderElectionHandler getHandler() {
		return handler;
	}

	/**
	 * abstraction of notification in the communication
	 * 
	 * @param listener
	 */
	public void addListener(MonitorListener listener) {
		if (handler == null && !listeners.contains(listener)) {
			listeners.add(listener);
			return;
		}

		try {
			handler.addListener(listener);
		} catch (Exception e) {
			logger.error("failed to add listener", e);
		}
	}

	public void release() {
		logger.warn("EelctionMonitor releasing resources");
		System.out.println("inside release finction in election monitor");

	//	System.out.println("##############################################################");
	//  System.out.println("whoami"+whoami);
	//	System.out.println("Host is"+host);
	//	System.out.println("port"+port);
		
		for (String id : handler.electionListeners.keySet()) {
			MonitorListener ml = handler.electionListeners.get(id);
				
			ml.connectionClosed();

			// hold back listeners to re-apply if the connection is
			// re-established.
			listeners.add(ml);
		}

		// TODO should wait a fixed time and use a listener to reset values;s
		channel = null;
		handler = null;
	}

	/**
	 * create connection to remote server
	 * 
	 * @return
	 */
	protected Channel connect() {
		// Start the connection attempt.
		
		//called from send method of same class
		if (channel == null) {
			System.out.println("channel is null");
			try {
				handler = new LeaderElectionHandler();
				ManagementInitializer mi = new ManagementInitializer(false);
			//	MonitorInitializer mi = new MonitorInitializer(handler, false);

				Bootstrap b = new Bootstrap();
				// @TODO newFixedThreadPool(2);
				b.group(group).channel(NioSocketChannel.class).handler(mi);
				b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);

				// Make the connection attempt.
				channel = b.connect(host, port).syncUninterruptibly();
				channel.awaitUninterruptibly(5000l);
				channel.channel().closeFuture().addListener(new MonitorClosedListener(this));

				if (N == Integer.MAX_VALUE)
					N = 1;
				else
					N++;

				// add listeners waiting to be added
				if (listeners.size() > 0) {
					for (MonitorListener ml : listeners)
						handler.addListener(ml);
					listeners.clear();
				}
			} catch (Exception ex) {
				logger.debug("failed to initialize the election connection");
				// logger.error("failed to initialize the heartbeat connection",
				// ex);
			}
		}

		if (channel != null && channel.isDone() && channel.isSuccess())
			return channel.channel();
		else
			throw new RuntimeException("Not able to establish connection to server");
	}

	public boolean isConnected() {
		if (channel == null)
			return false;
		else
			return channel.channel().isOpen();
	}

	public String getNodeInfo() {
		if (host != null)
			return host + ":" + port;
		else
			return "Unknown";
	}
	
	public boolean sendMessage(GeneratedMessage msg){
		
		boolean rtn = false;
		try {
			Channel ch = connect();

			/*logger.info("sending mgmt join message");
			Network.Builder n = Network.newBuilder();
			n.setNodeId("mgmt-" + whoami + "." + N);
			n.setAction(NetworkAction.NODEJOIN);*/
			
			/*Management.Builder le = Management.newBuilder(); 
			LeaderElection.Builder leb = LeaderElection.newBuilder();
			leb.setBallotId("123");
			leb.setDesc("Testing leader lection");
			leb.setNodeId("99");
			leb.setVote(LeaderElection.VoteAction.NOMINATE);
			
			
			Management.Builder m = Management.newBuilder();
			m.setElection(leb);*/
			ch.writeAndFlush(msg);
			rtn = true;
			logger.info("leader election message sent");
		} catch (Exception e) {
			// logger.error("could not send connect to node", e);
		}

		return rtn;
	}

	/**
	 * request the node to send heartbeats.
	 * 
	 * @return did a connect and message succeed
	 *//*
	public boolean startHeartbeat() {
		// the join will initiate the other node's heartbeatMgr to reply to
		// this node's (caller) listeners.

		boolean rtn = false;
		try {
			Channel ch = connect();

			logger.info("sending mgmt join message");
			Network.Builder n = Network.newBuilder();
			n.setNodeId("mgmt-" + whoami + "." + N);
			n.setAction(NetworkAction.NODEJOIN);
			
			Management.Builder le = Management.newBuilder(); 
			LeaderElection.Builder leb = LeaderElection.newBuilder();
			leb.setBallotId("123");
			leb.setDesc("Testing leader lection");
			leb.setNodeId("99");
			leb.setVote(LeaderElection.VoteAction.NOMINATE);
			
			
			Management.Builder m = Management.newBuilder();
			m.setElection(leb);
			ch.writeAndFlush(m.build());
			rtn = true;
			logger.info("join message sent");
		} catch (Exception e) {
			// logger.error("could not send connect to node", e);
		}

		return rtn;
	}*/

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	/**
	 * for demo application only - this will enter a loop waiting for
	 * heartbeatMgr messages.
	 * 
	 * Notes:
	 * <ol>
	 * <li>this method is not used by the servers
	 * <li>blocks if connection is created otherwise, it returns if the node is
	 * not available.
	 * </ol>
	 */
	/*public void waitForever() {
		try {
			boolean connected = startHeartbeat();
			while (connected) {
				Thread.sleep(2000);
			}
			logger.info("---> trying to connect heartbeat");
		} catch (Exception e) {
			// e.printStackTrace();
		}
	}*/

	/**
	 * Called when the channel tied to the monitor closes. Usage:
	 * 
	 */
	public static class MonitorClosedListener implements ChannelFutureListener {
		private LeaderElectionMonitor monitor;
// called from connect method of same class
		public MonitorClosedListener(LeaderElectionMonitor monitor) {
			this.monitor = monitor;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			monitor.release();
			System.out.println("inside Operation complete in ElectionMonitor");
		}
	}

	
/*	public static void main(String[] args) throws InterruptedException {
		
		ElectionMonitor monitor = new ElectionMonitor("Test", "localhost", 5670);
		monitor.addListener(new HeartPrintListener(null));
	//	monitor.waitForever();
		int i = 5;
		while(i!=0){
			System.out.println("Sending message");
		monitor.startHeartbeat();
		Thread.sleep(5000);
		i--;
		}
		
	}*/
}