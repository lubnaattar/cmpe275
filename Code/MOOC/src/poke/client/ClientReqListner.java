package poke.client;

	import org.slf4j.Logger;
	import org.slf4j.LoggerFactory;

	import poke.client.comm.CommListener;
	import poke.client.util.ClientUtil;
	import eye.Comm.*;

public class ClientReqListner implements   CommListener {
	
 	protected static Logger logger = LoggerFactory.getLogger("connect");

		private String id;

		public ClientReqListner(String id) {
			this.id = id;
		}

		@Override
		public String getListenerID() {
			return id;
		}

		@Override
		public void onMessage(eye.Comm.Request msg) {
		
			 if (msg.getHeader().getRoutingId().getNumber() == Header.Routing.JOBS_VALUE) {
				  
				logger.info("inside on message of jobs");   
				ClientUtil.printCourseList(msg); 
			} 
		}
	}


