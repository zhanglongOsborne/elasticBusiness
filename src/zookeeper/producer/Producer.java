package zookeeper.producer;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import zookeeper.connect.ZkClient;
import zookeeper.utils.Utils;

public class Producer implements Watcher{
	private ZkClient zk;
	private ProducerWatcher proWatcher;
	private String producerPath;
	private ArrayList<String>initialBussinessList;
	
	public Producer(ZkClient zKeeper,String producerPath,ArrayList<String>bussinessList) {
		// TODO Auto-generated constructor stub
		super();
		this.zk = zKeeper;
		this.producerPath = producerPath;
		this.proWatcher = new ProducerWatcher(zk, producerPath);
		this.initialBussinessList = bussinessList;
	}
	
	public boolean createNewProducer() throws KeeperException, InterruptedException{
		if(zk.getzKeeper().create(producerPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)==producerPath)
			return true;
		return false;
	}
	
	public void deleProducer() throws KeeperException, InterruptedException{
		Utils.recurseDeleteNode(zk, producerPath);
	}
	
	public void addInitialBusiness() throws KeeperException, InterruptedException{
		String path;
		for(String tmp:initialBussinessList){
			path = producerPath+"/"+tmp;
			zk.getzKeeper().create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		}
	}
	
	public void run() throws KeeperException, InterruptedException{
		addInitialBusiness();
		proWatcher.initOwnedBussinessList(initialBussinessList);
		proWatcher.setAllStableWatcher();
	}
	
	public void reConnect() throws KeeperException, InterruptedException{
		if(zk.getzKeeper().exists(producerPath, false) != null){
			
		}else{
			run();
		}
	}
	
	public Collection<String> getConsumerList(String producerName){
		return proWatcher.getConsumerListByBussinessKey(producerName);
	}
	

	@Override
	public void process(WatchedEvent arg0) {
		// TODO Auto-generated method stub
		
	}
}
