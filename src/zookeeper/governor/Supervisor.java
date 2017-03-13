package zookeeper.governor;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import zookeeper.connect.ZkClient;
import zookeeper.consumer.BusinessSetNodeWatcher;
import zookeeper.producer.ProducerSetWatcher;

public class Supervisor {
	private ZkClient zk;
	private String producerSetPath;
	private String bussinessSetPath;
	
	public Supervisor(ZkClient zk, String producerSetPath, String bussinessSetPath) {
		super();
		this.zk = zk;
		this.producerSetPath = producerSetPath;
		this.bussinessSetPath = bussinessSetPath;
	}
	
	public void run(){
		BusinessSetNodeWatcher bussinessSetWatcher = new BusinessSetNodeWatcher(zk,bussinessSetPath);
		ProducerSetWatcher producerSetWatcher = new ProducerSetWatcher(zk, producerSetPath);
		try {
			zk.getzKeeper().exists(bussinessSetPath, bussinessSetWatcher);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			zk.getzKeeper().exists(producerSetPath,producerSetWatcher);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
