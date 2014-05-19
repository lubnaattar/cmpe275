package poke.server.management.managers;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.queue.PerChannelQueue;
import poke.server.resources.ResourceFactory;
import eye.Comm.JobBid;
import eye.Comm.JobProposal;
import eye.Comm.Management;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * The job manager class is used by the system to assess and vote on a job. This
 * is used to ensure leveling of the servers take into account the diversity of
 * the network.
 * 
 * @author gash
 * 
 */
public class JobManager {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<JobManager> instance = new AtomicReference<JobManager>();
	JobBidWorker jobBidworkerThread = null;
	private class PerChannelQueueandJobOperation
	{
		private PerChannelQueue perChannelQueue;
		private Management jobProposal;
		public PerChannelQueueandJobOperation(PerChannelQueue perChannelQueue, Management jobProposal)
		{
			this.perChannelQueue = perChannelQueue;
			this.jobProposal = jobProposal;
		}
		public PerChannelQueue getPCQ()
		{
			return perChannelQueue;
		}
		public Management getJobProposal()
		{
			return jobProposal;
		}

	}
	//Map that will hold PerChannelQueue reference and JobProposal with JobId until response is sent to PCQ
	private Map<String,PerChannelQueueandJobOperation> queueingJobProposal = new HashMap<String,PerChannelQueueandJobOperation>();
	//All the incoming jobbid will be queued here, if i am the leader
	private Queue<JobBid> queueingJobBid = new LinkedList<JobBid>();
	//final map which will be processed and single JobBid will be sent to PerChannelQueue
	private Map<String,ArrayList<JobBid>> mappingJobBid = new HashMap<String,ArrayList<JobBid>>(); 
	//TODO Timer Map, checks if time for bid expired then remove bid and send failure to client
	private Map<String,Long> map_JobIdToTime = new HashMap<String,Long>();
	private String nodeId;


	public static JobManager getInstance(String id) {
		instance.compareAndSet(null, new JobManager(id));
		return instance.get();
	}

	public static JobManager getInstance() {
		return instance.get();
	}

	public JobManager(String nodeId) {
		logger.info("****************Job Manager Created***************");
		this.nodeId = nodeId;
		init();
	}
	protected void init()
	{
		//if(isLeader())
			//(new JobBidWorker(this)).start();
	}

	public boolean isLeader()
	{
		if(nodeId.equals(ElectionManager.getLeaderNodeId()))
			return true;
		else
			return false;
	}
	/**
	 * : Put new Job Proposal to be sent to other Servers Must be thread safe
	 * Accessed by PeChannelQueue
	 */
	public synchronized boolean submitJobProposal(PerChannelQueue sq, Management jbreq) {
			logger.info(" ******************* Job Proposal recieved, sending to channel *****************");
		queueingJobProposal.put(jbreq.getJobPropose().getJobId(), new PerChannelQueueandJobOperation(sq,jbreq));
		sendResponse(jbreq);
		return true;
	}
	/**
	 * If a bid is received from a node, forward the bid request unchanged 
	 * If I am a leader, add the bidding to my Queue
	 * @param mreq
	 */
	public void sendResponse(Management mreq)
	{
		for (HeartbeatData hd : HeartbeatManager.getInstance().getOutgoingQueue().values()) {
			logger.info("JobProposal Request beat (" + nodeId + ") sent to "
					+ hd.getNodeId() + " at " + hd.getHost()
					+ hd.channel.remoteAddress());
			try {
				if (hd.channel.isWritable()) {
					hd.channel.writeAndFlush(mreq);
				}
			} catch (Exception e) {
				logger.error("Failed  to send  for " + hd.getNodeId() + " at " + hd.getHost(), e);
			}
		}
	}

	/**
	 *  : a new job proposal has been sent out that I need to evaluate if I can run it
	 * 
	 * @param req
	 *            The proposal
	 */
	public void processRequest(JobProposal req) {
		if(isLeader())
		{
			//do nothing
			return;
		}
		logger.info("Received a Job Proposal");
		if (req == null) {
			logger.info("No Job Proposal request received..!");
			return;
		} else {
			logger.info("Owner of the Job Proposal : " + req.getOwnerId());
			logger.info("Job ID Received : " + req.getJobId());
			logger.info("I start to bid for the job..!");
			//startJobBidding(nodeId, req.getOwnerId(), req.getJobId());
			startJobBidding(req);
			//forward proposal request too
			Management.Builder b = Management.newBuilder();
			b.setJobPropose(req);
			sendResponse(b.build());
		}
	}

	/**
	 * a job bid for my job
	 * 
	 * @param req
	 *            The bid
	 */
	public void processRequest(JobBid req) {
		//If I am the leader, process Job Bid else forward

		if (req == null) {
			logger.info("No job bidding request received..!");
			return;
		} else {
			logger.info("Job bidding request received on channel..!");
		}

		if(isLeader())
		{
			if(jobBidworkerThread == null)
			{
				jobBidworkerThread = new JobBidWorker(this);
				jobBidworkerThread.start();
			}
			else if(!jobBidworkerThread.isAlive())
			{
				jobBidworkerThread.start();
			}
				queueingJobBid.add(req);
		}
		else
		{
			Management.Builder b = Management.newBuilder();
			b.setJobBid(req);
			sendResponse(b.build());
		}
	}

	/**
	 * Custom method for bidding for the proposed job
	 */
	public void startJobBidding(JobProposal jpReq) {
		
		for (HeartbeatData hd : HeartbeatManager.getInstance()
				.getOutgoingQueue().values()) {
			logger.info("Job proposal request on (" + nodeId + ") sent to "
					+ hd.getNodeId() + " at " + hd.getHost()
					+ hd.channel.remoteAddress());

			try {
				// sending job proposal request for bidding
				JobBid.Builder jb = JobBid.newBuilder();
				
				//setting the bid randomly
				int min = 1, max = 10;
				int bid = min + (int)(Math.random() * ((max - min) + 1));
				jb.setBid(bid);
				
				//set own node ID as the owner for this bid
				String bidOwner = ResourceFactory.getInstance().getCfg().getServer().getProperty("node.id");
				
				jb.setOwnerId(Integer.parseInt(bidOwner));
				jb.setJobId(jpReq.getJobId());
				jb.setNameSpace(jpReq.getNameSpace());

				Management.Builder b = Management.newBuilder();
				b.setJobBid(jb);
				if (hd.channel.isWritable()) {
					hd.channel.writeAndFlush(b.build());
				}

			} catch (Exception e) {
				logger.error(
						"Failed  to send bidding request for " + hd.getNodeId()
						+ " at " + hd.getHost(), e);
			}

		}
	}

	/**
	 * Class for receiving all the bids from all the nodes in the cluster.
	 * If no bids are present in the job_bid queue, then Thread will wait.
	 * Once all the bids are received from the nodes, the 
	 * @author 
	 *
	 */

	protected class JobBidWorker extends Thread{
		JobManager jobManager;
		public JobBidWorker(JobManager jm)
		{
			this.jobManager = jm;
		}
		public JobBid processJobBids(ArrayList<JobBid> jobBids)
		{
			JobBid finalJobBidding = null;
			int size = jobBids.size();
			for(int i = 0; i< size;i++)
			{
				JobBid jb = jobBids.remove(0);
				if(finalJobBidding == null)
				{
					finalJobBidding = jb;
				}
				else
				{
					if(finalJobBidding.getBid() < jb.getBid())
					{
						finalJobBidding = jb;
					}
				}
			}
			logger.info("The highest selected bid is : "+finalJobBidding.getBid());
			return finalJobBidding;
		}
		@Override
		public void run() {
			logger.info("Job bid worker started");
			while(true && isLeader())
			{
				//logger.info("Job Bid worker running..!!");
				if(jobManager.queueingJobBid.isEmpty())
				{
					//TODO can check for time here
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				else
				{
					JobBid req = jobManager.queueingJobBid.remove();
					String jobId = req.getJobId();
					if(!jobManager.mappingJobBid.containsKey(jobId))
					{
						ArrayList<JobBid> jobBids = new ArrayList<JobBid>();
						jobBids.add(req);
						jobManager.mappingJobBid.put(jobId,jobBids);
					}
					else
					{
						logger.info("Adding bid to hash map, Job Id: " + jobId + " Map Size: " + jobManager.mappingJobBid.size());
						ArrayList<JobBid> jobBids = jobManager.mappingJobBid.get(jobId);
						if(jobBids.size() >= Integer.parseInt(ResourceFactory.getCfg().getServer().getProperty("total_nodes"))-2)
						{
							logger.info("Selected one bid, sending it to PCQ");
							jobBids.add(req);
							JobBid JB = processJobBids(jobBids);
							PerChannelQueue pcq = jobManager.queueingJobProposal.get(jobId).getPCQ();
							pcq.putBidResponse(JB);
							jobManager.queueingJobProposal.remove(jobId);
							jobManager.mappingJobBid.remove(jobId);
							jobBids = null;
						}
						else
						{
							jobBids.add(req);
							jobManager.mappingJobBid.put(jobId,jobBids);
						}
					}

				}
			}
		}
	}
}
