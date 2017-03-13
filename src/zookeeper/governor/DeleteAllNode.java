package zookeeper.governor;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import zookeeper.connect.ZkClient;
import zookeeper.utils.Utils;

public class DeleteAllNode {
	public static void main(String [] args) throws IOException, KeeperException, InterruptedException{
		String host = "localhost:2181";
		int sessionTimeout = 2000;
		Watcher connectWatcher = new Watcher() {
			
			@Override
			public void process(WatchedEvent arg0) {
				// TODO Auto-generated method stub
				System.out.println("the connectwatcher gives information!");
			}
		};
		ZkClient zk= new ZkClient(host, sessionTimeout);
		zk.connect();
		
		String productSetPath = "/zookeeper/producerSet";
		String businessSetPath = "/zookeeper/businessSet";
		Utils.recurseDeleteNode(zk, productSetPath);
		Utils.recurseDeleteNode(zk, businessSetPath);
	}
}
