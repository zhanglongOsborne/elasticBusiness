package zookeeper.consumer;


import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import org.apache.zookeeper.ZooKeeper;

import zookeeper.connect.ZkClient;

import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.KeeperException.Code;

public class KeepCalmThread implements Runnable, VoidCallback{
	private HashMap<String,Integer> goneNodeMap;
	private HashMap<String, Integer> inCreatingMap;
	private int loopTime;
	private ZkClient zk;
	private Collection<String>tempConsumerList;
	private String stableNodePath;
	private String tempNodePath;

	private boolean tempNodeExist = true;
	
	
	public KeepCalmThread(ZkClient zk,int looptime,Collection<String>tempConsumerList,String tempNodePath,String stableNodePath) {
		goneNodeMap = new HashMap<String,Integer>();
		inCreatingMap = new HashMap<String,Integer>();
		this.zk = zk;
		this.loopTime = looptime;
		this.tempConsumerList = tempConsumerList;
		this.stableNodePath = stableNodePath;
		this.tempNodePath = tempNodePath;
		
	}
	
	public void addNodeToGoneMap(String znode,int sign){
		synchronized (goneNodeMap) {
			if(goneNodeMap.containsKey(znode)){
				goneNodeMap.replace(znode, 1);
			}else{
				goneNodeMap.put(znode, 1);
			}
		}
		
	}
	
	public boolean deleNodeFromGoneMap(String znode){
		synchronized (goneNodeMap) {
			if(goneNodeMap.containsKey(znode)){
				goneNodeMap.remove(znode);
				return true;
			}
		}
		return false;
	}
	
	public void addNodeToCreatingMap(String creatingNode,int sign){
		synchronized (inCreatingMap) {
			if(inCreatingMap.containsKey(creatingNode)){
				inCreatingMap.replace(creatingNode, 0);
			}else{
				//System.out.println("add the new node to creatingMap:"+stableNodePath+"/"+creatingNode);
				inCreatingMap.put(creatingNode, 1);
			}
		}
	}
	
	public boolean deleNodeFromCreatingMap(String creatingNode){
		synchronized (inCreatingMap) {
			if(inCreatingMap.containsKey(creatingNode)){
				inCreatingMap.remove(creatingNode);
				return true;
			}
		}
		return false;
	}
	
	public void checkTrashNode()  {
		Iterator<Map.Entry<String, Integer>> iterator = goneNodeMap.entrySet().iterator();
		String key;
		int value;
		/*
		 * 这里存在的问题：如果temNodePath不存在了，也就是通过其他方式将该节点删除了。
		 */
		
		List<String> tempChildren = null;
		try {
			tempChildren = zk.getzKeeper().getChildren(tempNodePath, false);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			tempNodeExist = false;
			return;
			//e.printStackTrace();
		}
		while(iterator.hasNext()){
			Map.Entry<String, Integer> entry = iterator.next();
			key = entry.getKey();
			value = entry.getValue();
			if(value == 0){
				if(tempChildren.contains(key)){
					synchronized (goneNodeMap) {
						//goneNodeMap.remove(key);
						iterator.remove();
					}
				}else{
					String deleNode = stableNodePath+"/"+key;
					zk.getzKeeper().delete(deleNode, -1, this, null);//这里使用回调函数。回调函数需要将temConsumerList中的节点删除，还需要将
													//goneNodeMap中的节点删除。
					synchronized (goneNodeMap) {
						//goneNodeMap.remove(key);
						iterator.remove();
					}
				}
				
			}else{
				goneNodeMap.replace(key, 0);
			}
		}
	}
	
	public void checkCreatingNode() {
		Iterator<Map.Entry<String, Integer>> iterator = inCreatingMap.entrySet().iterator();
		String key;
		int value;
		List<String> tempChildren = null;
		try {
			tempChildren = zk.getzKeeper().getChildren(tempNodePath, false);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			tempNodeExist = false;
			return;
			//e.printStackTrace();
		}
		while(iterator.hasNext()){
			Map.Entry<String, Integer> entry = iterator.next();
			key = entry.getKey();
			value = entry.getValue();
			if(tempChildren.contains(key)){
				if(value == 0){
					try {
						zk.getzKeeper().create(stableNodePath+"/"+key, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					} catch (KeeperException | InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//System.out.println("create the new node :"+result);
					synchronized (inCreatingMap) {
						iterator.remove();
					}
				}else{
					synchronized (inCreatingMap) {
						inCreatingMap.replace(key, 0);
					}
				}
			}else{
				if(value == 0){
					synchronized (inCreatingMap) {
						inCreatingMap.replace(key, 1);
					}
				}else{
					synchronized (inCreatingMap) {
						iterator.remove();
					}
				}
			}
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(tempNodeExist){
			//System.out.println("loop in goneNode!");
			try {
				Thread.sleep(loopTime);
				//System.out.println("the thread checking!");
				checkTrashNode();
				checkCreatingNode();
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx) {
		// TODO Auto-generated method stub
		//注意delete的节点是stable节点的子节点。
		switch(rc){
		case Code.Ok:
			String tempPath = path.substring(path.lastIndexOf("/"));
			synchronized (tempConsumerList) {
				tempConsumerList.remove(tempPath);
			}
			
			
			break;
		case Code.AuthFailed:
			System.out.println("delete node failed! authfailed!");
			break;
		case Code.NoNode:
			System.out.println("the node you want to delete is not existed!");
			break;
		case Code.SessionExpired:
			System.out.println("session expired!");
			break;
		default:
			System.out.println("we failed to delete the node!");
		}
		
	}
	
}
