package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_TTL;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryTypeList(List<ShopType> typeList) {
        // 从redis中查询店铺类型数据
        String shopTypeJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE_KEY);
        List<ShopType> shopTypeList = null;
        // 判断缓存是否命中
        if (StrUtil.isNotBlank(shopTypeJson)) {
            // 缓存命中，直接返回缓存数据
            shopTypeList = JSONUtil.toList(shopTypeJson, ShopType.class);
            return Result.ok(shopTypeList);
        }
        // 缓存未命中，查询数据库
        shopTypeList = this.query().orderByAsc("sort").list();
        // 判断数据库中是否存在该数据
        if (shopTypeList == null) {
            // 数据库中不存在该数据，返回失败信息
            return Result.fail("店铺类型不存在");
        }
        // 数据库中的数据存在，写入Redis，并返回查询的数据
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_TYPE_KEY, JSONUtil.toJsonStr(shopTypeList), CACHE_SHOP_TYPE_TTL, TimeUnit.MINUTES);
        // 将数据库查到的数据返回
        return Result.ok(shopTypeList);
    }
}
