package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {

    private static final Long BEGIN_TIMESTAMP =1640995200L;//开始时间
    private static final int COUNT_BITS=32;//序列号位数

    private StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    public Long nextId(String keyPrefix){
        //1生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long newSecond=now.toEpochSecond(ZoneOffset.UTC);
        long timestamp=newSecond-BEGIN_TIMESTAMP;
        //2生成序列号
        //2.1获取当前时间，同步到天
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:mm:dd"));
        //2.2自增长
        long count=stringRedisTemplate.opsForValue().increment("icr"+keyPrefix+":"+date);
        //3拼接并返回
        return timestamp<<COUNT_BITS|count;
// count 本身就是十进制数，但在计算机内部以二进制形式存储。在按位或运算时，count 的二进制表示会被直接填充到左移后的 timestamp 空出的位置上。
    }
}
