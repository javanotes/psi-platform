package com.reactiveminds.psi.streams.txn;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reactiveminds.psi.common.err.InternalOperationFailed;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @Deprecated
 * @UnderConstruction
 */
class ZKeeperTransactionManager implements TwoPCTransactionManager {

    private static final Logger log = LoggerFactory.getLogger(ZKeeperTransactionManager.class);
    private ZooKeeper zclient;
    private String host;

    private String createNode(final String node, byte[] data, final boolean watch, final boolean ephimeral) {
        String createdNodePath = null;
        try {

            final Stat nodeStat =  zclient.exists(node, watch);

            if(nodeStat == null) {
                createdNodePath = zclient.create(node, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, (ephimeral ?  CreateMode.EPHEMERAL_SEQUENTIAL : CreateMode.PERSISTENT));
            } else {
                createdNodePath = node;
            }

        } catch (KeeperException | InterruptedException e) {
            throw new InternalOperationFailed("Unable to call zookeeper", e);
        }

        return createdNodePath;
    }

    private boolean watchNode(final String node, final boolean watch) {

        boolean watched = false;
        try {
            final Stat nodeStat =  zclient.exists(node, watch);

            if(nodeStat != null) {
                watched = true;
            }

        } catch (KeeperException | InterruptedException e) {
            throw new InternalOperationFailed("Unable to call zookeeper", e);
        }

        return watched;
    }

    private List<String> getChildren(final String node, final boolean watch) {

        List<String> childNodes = null;

        try {
            childNodes = zclient.getChildren(node, watch);
        } catch (KeeperException | InterruptedException e) {
            throw new InternalOperationFailed("Unable to call zookeeper", e);
        }

        return childNodes;
    }

    public static final String TXN_ZNODE = "/psi/txn/";
    public static final String MAP_ZNODE = "/p_";

    private static ObjectMapper objectMapper = new ObjectMapper();
    static <T> byte[] objectToBytes(T t){
        try {
            return objectMapper.writeValueAsBytes(t);
        } catch (JsonProcessingException e) {
            throw new InternalOperationFailed("json ser error", e);
        }
    }
    @PostConstruct
    void init() throws IOException, InterruptedException {
        CountDownLatch connectionLatch = new CountDownLatch(1);
        zclient = new ZooKeeper(host, 2000, new Watcher() {
            public void process(WatchedEvent we) {
                if (we.getState() == Event.KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }
            }
        });
        connectionLatch.await();

    }
    @PreDestroy
    void destroy() throws InterruptedException {
        zclient.close();
    }

    @Override
    public boolean tryElectCoordinator(String id, String map) {
        String txnNode = createNode(TXN_ZNODE + id, new byte[0], false, false);
        ResourceData resourceData = new ResourceData();
        resourceData.setMap(map);
        String mapNode = createNode(txnNode  + MAP_ZNODE, objectToBytes(resourceData), false, true);

        List<String> children = getChildren(txnNode, false);
        Collections.sort(children);

        if(children.get(0).equals(mapNode)){
            return true;
        }
        return false;
    }

    @Override
    public void prepare(String txnId) {

    }

    @Override
    public void commit(String txnId) {

    }

    @Override
    public void rollback(String txnId) {

    }
}
