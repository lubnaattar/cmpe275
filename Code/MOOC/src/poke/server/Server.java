/*
 * copyright 2014, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.server;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ChannelFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import eye.Comm.LeaderElection;
import eye.Comm.Network;
import eye.Comm.Request;
import eye.Comm.LeaderElection.Builder;
import eye.Comm.LeaderElection.VoteAction;
import eye.Comm.Network.NetworkAction;
import eye.Comm.Management;
import poke.client.ClientPrintListener;
import poke.client.comm.CommConnection;
import poke.client.comm.CommListener;
import poke.demo.Monitor.HeartPrintListener;
import poke.monitor.LeaderElectionMonitor;
import poke.monitor.MonitorHandler;
import poke.monitor.MonitorInitializer;
import poke.monitor.MonitorListener;
import poke.monitor.HeartMonitor.MonitorClosedListener;
import poke.server.conf.JsonUtil;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.management.ManagementInitializer;
import poke.server.management.ManagementQueue;
import poke.server.management.managers.ElectionManager;
import poke.server.management.managers.HeartbeatConnector;
import poke.server.management.managers.HeartbeatData;
import poke.server.management.managers.HeartbeatManager;
import poke.server.management.managers.JobManager;
import poke.server.management.managers.NetworkManager;
import poke.server.resources.ResourceFactory;
import poke.server.storage.MongoDBDAO;

/**
 * Note high surges of messages can close down the channel if the handler cannot
 * process the messages fast enough. This design supports message surges that
 * exceed the processing capacity of the server through a second thread pool
 * (per connection or per server) that performs the work. Netty's boss and
 * worker threads only processes new connections and forwarding requests.
 * <p>
 * Reference Proactor pattern for additional information.
 * 
 * @author gash
 * 
 */
public class Server {
	protected static Logger logger = LoggerFactory.getLogger("server");

	protected static ChannelGroup allChannels;
	protected static HashMap<Integer, ServerBootstrap> bootstrap = new HashMap<Integer, ServerBootstrap>();
	// protected static ConcurrentHashMap<String, String> allNodeConfDetails=new
	// ConcurrentHashMap<String, String>();
	public static TreeMap<String, String> allNodeConfDeatils = new TreeMap<String, String>();
	//public static TreeMap<String, String> allNodeDeatils = new TreeMap<String, String>();
	
	 public static int[]nums; 
	protected ServerConf conf;
	protected JobManager jobMgr;
	protected NetworkManager networkMgr;
	protected HeartbeatManager heartbeatMgr;
	protected ElectionManager electionMgr;

	/**
	 * static because we need to get a handle to the factory from the shutdown
	 * resource
	 */
	public static void shutdown() {
		try {
			if (allChannels != null) {
				ChannelGroupFuture grp = allChannels.close();
				grp.awaitUninterruptibly(5, TimeUnit.SECONDS);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		logger.info("Server shutdown");
		System.exit(0);
	}

	public void release() {
		if (HeartbeatManager.getInstance() != null)
			HeartbeatManager.getInstance().release();
	}

	/**
	 * initialize the outward facing (public) interface
	 * 
	 * @param port
	 *            The port to listen to
	 */
	private static class StartCommunication implements Runnable {
		ServerConf conf;

		public StartCommunication(ServerConf conf) {
			this.conf = conf;
		}

		public void run() {
			// construct boss and worker threads (num threads = number of cores)

			EventLoopGroup bossGroup = new NioEventLoopGroup();
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			try {
				String str = conf.getServer().getProperty("port");
				if (str == null) {
					// TODO if multiple servers can be ran per node, assigning a
					// default
					// is not a good idea
					logger.warn("Using default port 5570, configuration contains no port number");
					str = "5570";
				}

				int port = Integer.parseInt(str);

				ServerBootstrap b = new ServerBootstrap();
				bootstrap.put(port, b);

				b.group(bossGroup, workerGroup);
				b.channel(NioServerSocketChannel.class);
				b.option(ChannelOption.SO_BACKLOG, 100);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);
				// b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR);

				boolean compressComm = false;
				b.childHandler(new ServerInitializer(compressComm));

				// Start the server.
				logger.info("Starting server "
						+ conf.getServer().getProperty("node.id")
						+ ", listening on port = " + port);
				ChannelFuture f = b.bind(port).syncUninterruptibly();

				// should use a future channel listener to do this step
				// allChannels.add(f.channel());

				// block until the server socket is closed.
				f.channel().closeFuture().sync();
			} catch (Exception ex) {
				// on bind().sync()
				logger.error("Failed to setup public handler.", ex);
			} finally {
				// Shut down all event loops to terminate all threads.
				bossGroup.shutdownGracefully();
				workerGroup.shutdownGracefully();
			}

			// We can also accept connections from a other ports (e.g., isolate
			// read
			// and writes)
		}
	}

	/**
	 * initialize the private network/interface
	 * 
	 * @param port
	 *            The port to listen to
	 */
	private static class StartManagement implements Runnable {
		private ServerConf conf;

		public StartManagement(ServerConf conf) {
			this.conf = conf;
		}

		public void run() {
			// construct boss and worker threads (num threads = number of cores)

			// UDP: not a good option as the message will be dropped

			EventLoopGroup bossGroup = new NioEventLoopGroup();
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			try {
				String str = conf.getServer().getProperty("port.mgmt");
				int mport = Integer.parseInt(str);

				ServerBootstrap b = new ServerBootstrap();
				bootstrap.put(mport, b);

				b.group(bossGroup, workerGroup);
				b.channel(NioServerSocketChannel.class);
				b.option(ChannelOption.SO_BACKLOG, 100);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);
				// b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR);

				boolean compressComm = false;
				b.childHandler(new ManagementInitializer(compressComm));

				// Start the server.

				logger.info("Starting mgmt "
						+ conf.getServer().getProperty("node.id")
						+ ", listening on port = " + mport);
				ChannelFuture f = b.bind(mport).syncUninterruptibly();

				// block until the server socket is closed.
				f.channel().closeFuture().sync();
			} catch (Exception ex) {
				// on bind().sync()
				logger.error("Failed to setup public handler.", ex);
			} finally {
				// Shut down all event loops to terminate all threads.
				bossGroup.shutdownGracefully();
				workerGroup.shutdownGracefully();
			}
		}
	}

	/**
	 * network.
	 * 
	 * TODO this should be refactored to use the conf file
	 */
	private void startManagers() {
		if (conf == null)
			return;

		// start the inbound and outbound manager worker threads
		ManagementQueue.startup();

		String myId = conf.getServer().getProperty("node.id");

		 allNodeConfDeatils=conf.getDesciption().getNode();
		 
		 
		 
			 
			 String a=allNodeConfDeatils.get("list");
			 
			 
			 
			 
			 String array[] = a.split(" ");
			nums = new int[array.length];
			   for(int i=0;i<array.length;i++){
			       try{
			          if(!array[i].trim().isEmpty()){
			              nums[i] = Integer.parseInt(array[i]);
			              System.out.println(nums[i]);
			              
			          }
			       }catch(Exception e){
			       }
			   }
			   
	
			 
			 
		
	
		 
		 //allNodeDeatils=conf.getDesciption().getNode();

		// create manager for network changes
		
		// pass conf also
		networkMgr = NetworkManager.getInstance(myId);

		// create manager for leader election
		String str = conf.getServer().getProperty("node.votes");
		int votes = 1;
		if (str != null)
			votes = Integer.parseInt(str);
		electionMgr = ElectionManager.getInstance(myId, votes);
		electionMgr.setServerConfig(conf);

		// create manager for accepting jobs
		jobMgr = JobManager.getInstance(myId);

		// establish nearest nodes and start receiving heartbeats
		heartbeatMgr = HeartbeatManager.getInstance(myId);
		for (NodeDesc nn : conf.getNearest().getNearestNodes().values()) {
			HeartbeatData node = new HeartbeatData(nn.getNodeId(),
					nn.getHost(), nn.getPort(), nn.getMgmtPort());
			HeartbeatConnector.getInstance().addConnectToThisNode(node);
		}
		heartbeatMgr.start();

		// manage heartbeatMgr connections
		HeartbeatConnector conn = HeartbeatConnector.getInstance();
		conn.start();

		logger.info("Server " + myId + ", managers initialized");
		// initiate voting
		// System.out.println("initiating the election.......electionManager.initiateElection() called in server");
		
		System.out.println("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
		 electionMgr.initiateElection();
		/*
		 * Thread thread = new Thread(new InitateCommunication(conf));
		 * thread.start();
		 */
	}

	/**
	 * 
	 */
	public void run() {
		if (conf == null) {
			logger.error("Missing configuration file");
			return;
		}

		String myId = conf.getServer().getProperty("node.id");
		logger.info("Initializing server " + myId);

		// storage initialization
		// TODO storage setup (e.g., connection to a database)

	/*	try {
			
			MongoClient client=new MongoClient("localhost",27017);
			DB db=client.getDB("moocdata");
			DBCollection collection=db.getCollection("students");
			
			BasicDBObject o=new BasicDBObject();
			o.put("1", "1");
			o.put("2", "1");
			o.put("3", "1");
			o.put("4", "1");
			o.put("5", "1");
			collection.insert(o);
			
			DBCursor cur=collection.find();
			while(cur.hasNext()){
				
				DBObject doc =cur.next();
				
				System.out.println(doc);
			}
			
			//MongoDBDAO mongoInstance = new MongoDBDAO();
			//mongoInstance.getDBConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
*/
		startManagers();

		StartManagement mgt = new StartManagement(conf);
		Thread mthread = new Thread(mgt);
		mthread.start();

		StartCommunication comm = new StartCommunication(conf);
		logger.info("Server " + myId + " ready");

		Thread cthread = new Thread(comm);
		cthread.start();
	}

	/**
	 * initialize the server with a configuration of it's resources
	 * 
	 * @param cfg
	 */
	public Server(File cfg) {
		init(cfg);

	}

	private void init(File cfg) {
		// resource initialization - how message are processed
		BufferedInputStream br = null;
		try {
			byte[] raw = new byte[(int) cfg.length()];
			br = new BufferedInputStream(new FileInputStream(cfg));
			br.read(raw);
			conf = JsonUtil.decode(new String(raw), ServerConf.class);
			ResourceFactory.initialize(conf);
		} catch (Exception e) {
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 1) {
			System.err.println("Usage: java "
					+ Server.class.getClass().getName() + " conf-file");
			System.exit(1);
		}

		File cfg = new File(args[0]);
		if (!cfg.exists()) {
			Server.logger.error("configuration file does not exist: " + cfg);
			System.exit(2);
		}

		Server svr = new Server(cfg);
		svr.run();
	}

}