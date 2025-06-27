package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.RedisData;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
//import org.springframework.data.redis.domain.geo.GeoReference;
//import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.*;


/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
   @Resource
   private StringRedisTemplate stringRedisTemplate;

   @Resource
   private CacheClient cacheClient;
   @Override


   public Result queryById(Long id) {
//          缓存穿透
//       Shop shop = cacheClient.queryWithPassThrough
//               (CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);

//       互斥锁解决缓存击穿
//       Shop shop = queryWithMutex(id);

//       用逻辑过期解决
       Shop shop = cacheClient.queryWithLogicalExpire
               (CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
       if(shop ==null){
           return Result.fail("店铺不存在");
       }
//       返回
       return Result.ok(shop);
   }



    //       用逻辑过期解决缓存击穿
//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
//    public Shop queryWithLogicalExpire(Long id){
//        String key=CACHE_SHOP_KEY + id;
//        //1从redis缓存中查询该用户
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //2判断是否存在
//        if (StrUtil.isBlank(shopJson)) {
//             //3不存在，直接返回null，去mysql
//            return null;
//        }
//        //TODO
//        //4命中，将json反序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        JSONObject data = (JSONObject) redisData.getData();
//        Shop shop = JSONUtil.toBean(data, Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//
//        //5判断是否过期
//
//        if (expireTime.isAfter(LocalDateTime.now())) {
//            //5.1.未过期，返回店铺信息
//            return shop;
//        }
//
//        //5.2已过期，需要缓存重建
//
//        String lockKey = RedisConstants.LOCK_SHOP_KEY+id;
//        //6缓存重建
//        //6.1获取互斥锁
//        boolean isLock=tryLock(lockKey);
//
//        //6。2判断是否获取成功
//        if(isLock){
//            //6.3成功，开启独立线程，实现缓存重建
//            CACHE_REBUILD_EXECUTOR.submit(()->{
//                try {
//                    this.saveShop2Redis(id,20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {unLock(lockKey);
//                }
//            });
//        }
//
//        //6.4返回过期的商铺信息
//
//
//
//           return shop;
//    }
    //       互斥锁解决缓存击穿
    /*public Shop queryWithMutex(Long id){
        String key=CACHE_SHOP_KEY + id;
        //从redis缓存中查询该用户
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //存在，直接返回

            return JSONUtil.toBean(shopJson, Shop.class);
        }

//       判断命中的是否为空值
        if(shopJson != null){
            return null;
        }

        //4实现缓存重建
        //4.1获取互斥锁
        String lockKey="lock:shop:"+id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2判断是否获取成功
            if (!isLock) {
                //4.3失败，休眠并重试
                Thread.sleep(50);
             return    queryWithMutex(id);
            }


            //4.4成功，根据id查询数据库
            shop = getById(id);
            Thread.sleep(200);

            //不存在，报错
            if (shop==null) {
    //           将空值写入redis
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);

                return null;
            }

            //存在，将商品数据写入缓存，返回商铺信息
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //释放互斥锁
            unLock(lockKey);
        }
        return shop;
    }*/
    //          缓存穿透
  /* public Shop queryWithPassThrough(Long id){
       String key=CACHE_SHOP_KEY + id;
       //从redis缓存中查询该用户
       String shopJson = stringRedisTemplate.opsForValue().get(key);
       //判断是否存在
       if (StrUtil.isNotBlank(shopJson)) {
           //存在，直接返回

           return JSONUtil.toBean(shopJson, Shop.class);
       }

//       判断命中的是否为空值
       if(shopJson != null){
           return null;
       }

       //不存在，根据id查询数据库
       Shop shop = getById(id);

       //不存在，报错
       if (shop==null) {
//           将空值写入redis
           stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);

           return null;
       }
       //存在，将商品数据写入缓存，返回商铺信息
       stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
       return shop;
   }*/
//   //        设置互斥锁
//   private boolean tryLock(String key){
//       Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
//       return BooleanUtil.isTrue(flag);
//   }
// //释放锁
//   private void unLock(String key){
//       stringRedisTemplate.delete(key);
//   }
//
//   //添加有逻辑过期时间对象的方法
//   public void saveShop2Redis(Long id,Long expireSeconds) throws InterruptedException {
//       Shop shop = getById(id);
//         Thread.sleep(200);
//
//       RedisData redisData = new RedisData();
//       redisData.setData(shop);
//       redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
//
//       stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
//   }

    @Override
    @Transactional
    public Result update(Shop shop) {

       Long id=shop.getId();
        if (id==null) {


            return Result.fail("店铺不能为空");
        }

        updateById(shop);

        stringRedisTemplate.delete(CACHE_SHOP_KEY+id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
       // 判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询，根据类型分页查询
            Page<Shop> page = query().eq("type_id", typeId).page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        // 计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        // 根据坐标查询redis，按照距离排序、分页查询。结果：shopId，maxDistance
        String geoKey = SHOP_GEO_KEY + typeId;
        // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE WITHHASH
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                geoKey,
                GeoReference.fromCoordinate(x, y),  // 查询以给定的经纬度为中心的圆形区域
                new Distance(10000),    // 查询10km范围内的店铺，单位默认为米
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)   // 分页查询0~end条
        );
        // 解析出id
        if (results == null) {
            // 未查到结果，返回错误
            return Result.fail("没有查到店铺");
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        // from跳过前面元素不足from个，跳过后集合为空，说明查完了没有下一页了，返回空集合
        if (list.size() <= from) {
            return Result.ok(Collections.emptyList());
        }
        // 截取from ~ end的部分，方法一：list.subList(from, end); 方法二：stream流的skip方法，跳过from前面的元素，从from开始，截取end-from个元素
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 获取店铺id（Member）
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 根据id查询店铺数据
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        // 遍历店铺数据，设置距离
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shops);
    }
}
