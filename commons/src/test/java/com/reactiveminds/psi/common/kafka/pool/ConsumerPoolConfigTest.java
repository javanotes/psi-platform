package com.reactiveminds.psi.common.kafka.pool;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Properties;

@RunWith(SpringRunner.class)
public class ConsumerPoolConfigTest {

    @Test
    public void testUsingBeanCopyWithPool(){
        KafkaConsumerPool<String,String> pool = new KafkaConsumerPool<>();
        Properties p = new Properties();
        int maxIdle = pool.getMaxIdle();
        p.setProperty("maxIdle", String.valueOf(maxIdle + 2) );
        pool.setPoolProperties(p, null);
        Assert.assertEquals(maxIdle + 2, pool.getMaxIdle());
    }
}
