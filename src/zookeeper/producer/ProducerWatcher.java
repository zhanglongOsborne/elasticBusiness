package zookeeper.producer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import zookeeper.connect.ZkClient;

public class ProducerWatcher implements Watcher{
	private ZkClient zk;
	private HashMap<String, StableNodeWatcher> ownedBussinessMap;
	private String producerPath;
	
	
	public ProducerWatcher(ZkClient zk, String producerPath) {
		super();
		this.zk = zk;
		this.producerPath = producerPath;
	}

	public void initOwnedBussinessList(ArrayList<String> bussinessList){
		ownedBussinessMap = new HashMap<>();
		Iterator<String> iterator = bussinessList.iterator();
		while(iterator.hasNext()){
			String key = iterator.next();
			if(ownedBussinessMap.containsKey(key)){
				System.out.println("the bussiness you insert is already exist!");
			}else{
				ownedBussinessMap.put(key, null);
			}
		}
	}
	
	public void addToOwnedBussinessList(String newBussiness,StableNodeWatcher stableNodeWatcher){
		if(!ownedBussinessMap.containsKey(newBussiness)){
			ownedBussinessMap.put(newBussiness,stableNodeWatcher);
		}else{
			System.out.println("the bussiness you insert is already exist!");
		}
		
	}
	
	public void removeFromOwnedBussinessList(String deleteBussiness){
		if(!ownedBussinessMap.containsKey(deleteBussiness)){
			System.out.println("the deleteBussiness is not exist!");
		}
		ownedBussinessMap.remove(deleteBussiness);
	}
	
	public void setAllStableWatcher(){
		Iterator<Map.Entry<String, StableNodeWatcher>> iterator = ownedBussinessMap.entrySet().iterator();
		String bussiness;
		String stableNode;
		while(iterator.hasNext()){
			bussiness = iterator.next().getKey();
			stableNode = "/zookeeper/businessSet/"+bussiness+"/stable";
			
			setStableWatcher(bussiness, stableNode);
		}
	}
	
	public void setStableWatcher(String bussinessName,String stableNodePath){
		StableNodeWatcher watcher = new StableNodeWatcher(zk,stableNodePath);
		ownedBussinessMap.put(bussinessName, watcher);
		try {
			zk.getzKeeper().exists(stableNodePath, watcher);//在这设置watcher
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Collection<String> getConsumerListByBussinessKey(String key){
		return ownedBussinessMap.get(key).getConsumerList();
	}

	@Override
	public void process(WatchedEvent arg0) {
		// TODO Auto-generated method stub
		
	}
}
