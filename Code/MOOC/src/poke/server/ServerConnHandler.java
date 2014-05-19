package poke.server;

import poke.client.comm.CommHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
/**
 * 
 * @author 
 * This class is not doing anything in current design
 * If ServerConnection receives any message in future this class will be used
 */
@CommHandler.Sharable
public class ServerConnHandler extends SimpleChannelInboundHandler<eye.Comm.Request>{

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, eye.Comm.Request msg) throws Exception {
		//System.out.println("ctx.channel().pipeline().toString()");
		//As per current design there will no message here
		onMessage(msg);
	}
	

	private void onMessage(eye.Comm.Request msg)
	{
		
	}
}
