package poke.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.management.managers.ElectionManager;
import poke.server.queue.PerChannelQueue;
import eye.Comm.Request;

public class JobOpManager {

	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<JobOpManager> instance = new AtomicReference<JobOpManager>();

	//JobManager : Store mapping between pcq and Job
	private class PCQandJob
	{
		private PerChannelQueue pcq;
		private Request jOpReq;
		public PCQandJob(PerChannelQueue pcq, Request jOpReq)
		{
			this.pcq = pcq;
			this.jOpReq = jOpReq;
		}
		public PerChannelQueue getPCQ()
		{
			return pcq;
		}
		public Request getJobOperation()
		{
			return jOpReq;
		}

	}
	//JobManager: Map that will hold PerChannelQueue reference and 
	//JobOperation with JonId until response is sent to PCQ
	private Map<String,PCQandJob> map_JobOperation = new HashMap<String,PCQandJob>();
	
	private String nodeId;
	ServerConnection serverConn = null;
	
	
	public static JobOpManager getInstance(String id) {
		instance.compareAndSet(null, new JobOpManager(id));
		return instance.get();
	}

	public static JobOpManager getInstance() {
		return instance.get();
	}

	public JobOpManager(String nodeId) {
		logger.info("***********Job Op Manager Created**********");
		this.nodeId = nodeId;
		init();
	}
	public void init()
	{
		serverConn = new ServerConnection();
	}

	public boolean isLeader()
	{
		if(nodeId.equals(ElectionManager.getLeaderNodeId()))
			return true;
		else
			return false;
	}
	
	public synchronized boolean submitJobOperation(PerChannelQueue sq, Request jOpReq) {
		logger.info("Job Operation recieved, store and send to channel");
		if(isLeader())
			map_JobOperation.put(jOpReq.getBody().getJobOp().getData().getJobId(), new PCQandJob(sq,jOpReq));
		//forwarding to next node, nothing will be handled here, just an interface to forward to next node
		sendResponse(jOpReq);
		return true;
	}
	
	public synchronized boolean submitJobStatus(Request req){
		if(isLeader())
		{
			String jobId = req.getBody().getJobStatus().getJobId();
			if(map_JobOperation.containsKey(jobId))
			{
				PerChannelQueue pcq = map_JobOperation.get(jobId).getPCQ();
				logger.info("Sending Response to Client PCQ");
				pcq.enqueueResponse(req, null);
			}
			else
			{
				logger.info("Job not found in Map, discard the response");
			}
		}
		else
		{
			sendResponse(req);
		}
		return true;
	}
	
	public void sendResponse(Request jOpReq)
	{
		//Send Message to ServerConnection
		try{
			logger.info("Inside JobOpManager.. Forwarding the request to ServerConncection");
			serverConn.sendMessage(jOpReq);
		}catch(Exception e)
		{
			logger.error("Exception: " ,e);
		}

	}

	
}