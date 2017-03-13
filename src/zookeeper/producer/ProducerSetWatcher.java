package zookeeper.producer;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import zookeeper.connect.ZkClient;

public class ProducerSetWatcher implements Watcher{
	private ZkClient zk;
	private Collection<String> producerSetList;
	private String producerSetNodePath;
	
	public ProducerSetWatcher(ZkClient zk, String producerSetNodePath) {
		super();
		this.zk = zk;
		this.producerSetNodePath = producerSetNodePath;
		initProducerSetList(zk, producerSetNodePath);
	}
	
	public void initProducerSetList(ZkClient zk,String path){
		producerSetList = new ArrayList<String>();
		try {
			producerSetList = zk.getzKeeper().getChildren(path, false);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	}

	private void processNodeDeleted(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("the producerSetNode is deleted!");
		
	}

	private void processNodeDataChanged(WatchedEvent event) {
		// TODO Auto-generated method stub
		
	}

	private void processNodeCreated(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("the producerSetNode is deleted!");
		
	}

	private void processNodeChildrenChanged(WatchedEvent event) throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		String changedPath = event.getPath();
		if(producerSetList.contains(changedPath)){
			if(zk.getzKeeper().exists(changedPath, false) == null){  //删除了节点。
				producerSetList.remove(changedPath);
			}
		}else{
			if(zk.getzKeeper().exists(changedPath, false) != null){ //增添了节点
				producerSetList.add(changedPath);
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
