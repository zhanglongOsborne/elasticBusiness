package zookeeper.consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import zookeeper.connect.ZkClient;

public class BusinessSetNodeWatcher implements Watcher{

	private ZkClient zk;
	private Collection<String> businessSetList;
	private String znode;
	
	
	public BusinessSetNodeWatcher(ZkClient zk, String znode) {
		super();
		this.zk = zk;
		this.znode = znode;
		initBusinessSetList(zk, znode);
	}
	
	public Collection<String> initBusinessSetList(ZkClient zk,String path){
    	businessSetList = new ArrayList<String>();
    	try {
			businessSetList = zk.getzKeeper().getChildren(path,false);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return businessSetList;
    }
	
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		/**
    	 * Event.EventType分为以下几个type：NodeChildrenChanged,NodeCreated,NodeDataChanged,NodeDeleted,
    	 * None.
    	 */
    	switch(event.getType()){
    	case None:
    		processConState(event);
    		break;
    	case NodeChildrenChanged:
    		try {
				processNodeChildrenChanged(event);
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		break;
    	case NodeCreated:
    		processNodeCreated(event);
    		break;
    	case NodeDataChanged:
    		processNodeDataChanged(event);
    		break;
    	case NodeDeleted:
    		processNodeDeleted(event);
    		break;
    	}
	}
	
	
	
	private void processNodeDeleted(WatchedEvent event) {
		// TODO Auto-generated method stub
    	System.out.println("the node you are watching is deleted!");
		
	}

	private void processNodeDataChanged(WatchedEvent event) {
		// TODO Auto-generated method stub
		
	}

	private void processNodeCreated(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("the node you are watching is created!");
		
	}

	private void processNodeChildrenChanged(WatchedEvent event) throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		String changedPath = event.getPath();
		if(businessSetList.contains(changedPath)){
			//可能是删除了业务，这时候需要使用exist进行确认。
			if(zk.getzKeeper().exists(changedPath, false) == null){
				//删除了该业务，需要在维护的list中删除该子节点。
				businessSetList.remove(changedPath);
			}
		}else {   //新建了业务节点。
			businessSetList.add(changedPath);
			//这时候需要查看消费者临时队列，然后将临时队列中的消费者添加到消费者集合中，并且设置一个watcher来观察临时队列。
			TemporaryNodeWatcher tempNodeWatcher = new TemporaryNodeWatcher();
			List<String> tmpComsumerList = zk.getzKeeper().getChildren(changedPath+"temporary", tempNodeWatcher);
			//接下来就是向消费者集合中写入永久节点。
			if(tmpComsumerList != null){
				String tmp;
				for(String childPath:tmpComsumerList){
					tmp = childPath.substring(childPath.lastIndexOf("/"));
					zk.getzKeeper().create(changedPath+"/stable"+tmp, null, ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
			}
			
		}
		
	}

	private void processConState(WatchedEvent event) {
		// TODO Auto-generated method stub
		switch(event.getState()){
		case AuthFailed://auth failed state
			System.out.println("你没有权限，请更改权限后再试");
			break;
		case Disconnected://the client is in the disconnected state.
			//重新进行连接，连接的过程需要设置一个timer来进行限制。
			break;
		case Expired://the server has expired this session.
			//这里需要进行重新连接处理，在出来的过程中连接一定次数或者连接一定时间后如果还是不能连接，说明网络存在问题。
			break;
		case SyncConnected:
			break;
		}
	}

}
