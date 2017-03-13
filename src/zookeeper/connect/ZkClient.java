package zookeeper.connect;

import java.io.IOException;
import java.lang.invoke.ConstantCallSite;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import com.google.common.base.Service.State;

/**
 * 
 * @author osborne
 *	这个类主要是用来和zookeeper server 建立连接并且保证连接一直处于
 *	活动状态，如果断开连接后，重新建立连接的作用。
 */

public class ZkClient {
	private String host;
	private int sessionTimeOut;
	private ZooKeeper zKeeper;
	
	private int reConTime = 0;
	private int TOLLERENT_RECONNECT_TIME = 10;
	public ZkClient(String host, int sessionTimeOut) {
		super();
		this.host = host;
		this.sessionTimeOut = sessionTimeOut;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getSessionTimeOut() {
		return sessionTimeOut;
	}
	public void setSessionTimeOut(int sessionTimeOut) {
		this.sessionTimeOut = sessionTimeOut;
	}
	public ZooKeeper getzKeeper() {
		return zKeeper;
	}
	
	public void connect() throws IOException{
		zKeeper = new ZooKeeper(host, sessionTimeOut, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				KeeperState state = event.getState();
				if(state == KeeperState.Expired || state == KeeperState.Disconnected){
					reConTime++;
					if(reConTime<=TOLLERENT_RECONNECT_TIME){
						try {
							connect();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		});
		if(zKeeper == null){
			reConTime++;
			if(reConTime>=TOLLERENT_RECONNECT_TIME){
				System.out.println("failed to connect to the host: "+host);
				return;
			}
				connect();
		}else{
			System.out.println("succeed to connect to the host:" +host);
			reConTime = 0;
		}
	}
	
	
	
}
