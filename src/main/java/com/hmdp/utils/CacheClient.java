package com.hmdp.utils;


import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.events.Event;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {

    private  final StringRedisTemplate stringRedisTemplate;
    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    //new redis的工具类
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }
    //new 有逻辑过期时间的redis的工具类
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));

        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //解决缓存穿透的查询（不仅包括shop，还能有其他）;
    /*1不知道方法的返回值，用泛型<R> R 替代；且需要调用方法时给我传过来
    2 不知道方法的前缀名（key==前缀名+id）
    3 不知道id的类型，因为不一定是Long
    4 查询数据库时，不一定调用getById(id)方法，所以要传这个方法过来；有参有返回值->Function<ID,R> ID是参数，R是返回值
    5 将商品数据写入缓存时，要设置不统一的时间，所以要传时间和单位
    */
    public <R,ID>R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key=keyPrefix + id;
        //从redis缓存中查询该用户
        String Json = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if (StrUtil.isNotBlank(Json)) {
            //存在，直接返回
            return JSONUtil.toBean(Json, type);
        }

//       判断命中的是否为空值
        if(Json != null){
            return null;
        }

        //不存在，根据id查询数据库
       R r = dbFallback.apply(id);

        //不存在，报错
        if (r==null) {
//           将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);

            return null;
        }
        //存在，将商品数据写入缓存，返回商铺信息
       this.set(key,r,time,unit);
        return r;
    }


    //       用逻辑过期解决缓存击穿
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public <R,ID>R queryWithLogicalExpire
            (String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key=keyPrefix + id;
        //1从redis缓存中查询该用户
        String Json = stringRedisTemplate.opsForValue().get(key);
        //2判断是否存在
        if (StrUtil.isBlank(Json)) {
            //3不存在，直接返回null，去mysql
            return null;
        }
        //TODO
        //4命中，将json反序列化为对象
        com.hmdp.entity.RedisData redisData = JSONUtil.toBean(Json, com.hmdp.entity.RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        //5判断是否过期

        if (expireTime.isAfter(LocalDateTime.now())) {
            //5.1.未过期，返回店铺信息
            return r;
        }

        //5.2已过期，需要缓存重建

        String lockKey = RedisConstants.LOCK_SHOP_KEY+id;
        //6缓存重建
        //6.1获取互斥锁
        boolean isLock=tryLock(lockKey);

        //6。2判断是否获取成功
        if(isLock){
            //6.3成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                   //查询数据库
                    R r1 = dbFallback.apply(id);
                    //重建缓存,写入redis
                    this.setWithLogicalExpire(key,r,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {unLock(lockKey);
                }
            });
        }

        //6.4返回过期的商铺信息
        return r;
    }
    //        设置互斥锁
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    //释放锁
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }
}
