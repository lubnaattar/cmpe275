package poke.monitor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessage;

public class LeaderElectionHandler extends SimpleChannelInboundHandler<eye.Comm.Management> {


	protected static Logger logger = LoggerFactory.getLogger("mgmt");

	protected ConcurrentMap<String, MonitorListener> electionListeners = new ConcurrentHashMap<String, MonitorListener>();
	private volatile Channel channel;

	public LeaderElectionHandler() {
	}

	public String getNodeId() {
		if (electionListeners.size() > 0)
			return electionListeners.values().iterator().next().getListenerID();
		else if (channel != null)
			return channel.localAddress().toString();
		else
			return String.valueOf(this.hashCode());
	}

	public void addListener(MonitorListener rLlistener) {
		if (rLlistener == null)
			return;

		electionListeners.putIfAbsent(rLlistener.getListenerID(), rLlistener);
	}

	public boolean send(GeneratedMessage msg) {
		// need argument context 
		// TODO a queue is needed to prevent overloading of the socket
		// connection. For the demonstration, we don't need it
		ChannelFuture cf = channel.writeAndFlush(msg);
		if (cf.isDone() && !cf.isSuccess()) {
			logger.error("failed to poke!");
			return false;
		}

		return true;
	}

	/**
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 * 
	 * @param msg
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, eye.Comm.Management msg) throws Exception {
		for (String id : electionListeners.keySet()) {
			MonitorListener ml = electionListeners.get(id);

			// TODO this may need to be delegated to a thread pool to allow
			// async processing of replies
			ml.onMessage(msg);
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		logger.error("monitor channel inactive");
		
		// TODO try to reconnect
		
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}


}
