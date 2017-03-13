package zookeeper.producer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import zookeeper.connect.ZkClient;

public class StableNodeWatcher implements Watcher{
	private Collection<String> consumerList;
	private ZkClient zk;
	private String stableNodePath;
	

	public StableNodeWatcher(ZkClient zk, String stableNodePath) {
		super();
		this.zk = zk;
		this.stableNodePath = stableNodePath;
		initConsumerList();
		
	}
	
	public StableNodeWatcher() {
		// TODO Auto-generated constructor stub
		
	}
	
	public boolean initConsumerList(){
		consumerList = new ArrayList<>();
		List<String> childrenList = null;
		try {
			childrenList = zk.getzKeeper().getChildren(stableNodePath, false);
			consumerList.addAll(childrenList);
			return true;
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
	}
	
	public Collection<String> getConsumerList(){
		synchronized (consumerList) {
			return consumerList;
		}
	}

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
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
		/**
		 * 下面是重新设置watcher
		 */
		try {
			zk.getzKeeper().exists(stableNodePath, this);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void processNodeDeleted(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("the stable node you watched is deleted!");
	}

	private void processNodeDataChanged(WatchedEvent event) {
		// TODO Auto-generated method stub
		
	}

	private void processNodeCreated(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("the stable node you watched is created!");
	}

	private void processNodeChildrenChanged(WatchedEvent event) throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("the stable node you watched is changed!");
		String changedPath = event.getPath();
		synchronized (consumerList) {
			if(consumerList.contains(changedPath)){
				if(zk.getzKeeper().exists(changedPath, false) == null){  //原来存在的节点被删除了。
					consumerList.remove(changedPath);
				}
			}else{
				if(zk.getzKeeper().exists(changedPath, false) != null){
					consumerList.add(changedPath);
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
