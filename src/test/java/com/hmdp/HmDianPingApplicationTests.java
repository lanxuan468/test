package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {


    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private CacheClient cacheClient;

    @Resource
    private RedisIdWorker redisIdWorker;
    private ExecutorService es=Executors.newFixedThreadPool(500);
    @Resource
    private StringRedisTemplate stringRedisTemplate;

/*
    @Test
    void testIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);

        Runnable task=()->{
            for (int i = 0; i < 100; i++) {
                Long id = redisIdWorker.nextId("order");
                System.out.println("id="+id);
            }
            latch.countDown();
        };
        long begin = System.currentTimeMillis();

        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }latch.await();
     Long end= System.currentTimeMillis();
        System.out.println("time="+(end-begin));
    }

    @Test

    void testSaveShop() throws InterruptedException {
        Shop shop = shopService.getById(1L);

        cacheClient.setWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY+1L,shop,10L, TimeUnit.SECONDS);
    }
    @Test
    void loadShopData() {
        // 查询店铺信息
        List<Shop> shops = shopService.list();
        // 把店铺分组，按照typeId分组，id一致的放到一个集合
        Map<Long, List<Shop>> groupByShopTypeToMap = shops.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        // 分批完成写入Redis
        for (Map.Entry<Long, List<Shop>> entry : groupByShopTypeToMap.entrySet()) {
            // 获取类型id
            Long typeId = entry.getKey();
            String geoKey = SHOP_GEO_KEY + typeId;
            // 获取同类型的店铺的集合
            List<Shop> shopList = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shopList.size());
            // 写入Redis的GEO GEOADD KEY 经度 纬度 Member
            for (Shop shop : shopList) {
                //stringRedisTemplate.opsForGeo().add(geoKey, new Point(shop.getX(), shop.getY()), shop.getId().toString());
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())
                ));
            }
            // 批量写入Redis的GEO
            stringRedisTemplate.opsForGeo().add(geoKey, locations);
        }
    }
    @Test
    void testHyperLogLog() {
        String[] values = new String[1000];
        for (int i = 0; i < 1000000; i++) {
            values[i % 1000] = "user_" + i;
            // 每1000次，添加一次，添加到Redis
            if (i % 1000 == 999) {
                stringRedisTemplate.opsForHyperLogLog().add("hl2", values);
            }
        }
        // 统计数量
        Long count = stringRedisTemplate.opsForHyperLogLog().size("hl2");
        System.out.println("count = " + count); // count = 997593
    }
*/


}
