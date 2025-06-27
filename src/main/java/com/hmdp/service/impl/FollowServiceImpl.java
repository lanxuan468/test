package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.FOLLOW_KEY_PREFIX;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private UserServiceImpl userService;
    @Override
    public Result setFollow(Long followId, boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        String key="follows:"+userId;
        if (isFollow) {
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followId);
            boolean isSuccess = save(follow);
            if (isSuccess) {
                stringRedisTemplate.opsForSet().add(key,followId.toString());
            }
        }else {
            boolean isSuccess = remove(new QueryWrapper<Follow>()
                    .eq("user_id", userId).eq("follow_user_id", followId));
            if (isSuccess) {
                stringRedisTemplate.opsForSet().remove(key,followId.toString());
            }
        }
        return Result.ok();
    }
    @Override
    public Result isFollow(Long followUserId) {
        Long userId = UserHolder.getUser().getId();
        // 查询是否关注 select count(*) from tb_follow where user_id = ? and follow_user_id = ?
        Integer count = lambdaQuery()
                .eq(Follow::getUserId, userId)
                .eq(Follow::getFollowUserId, followUserId)
                .count();
        // 判断是否关注
        return Result.ok(count > 0);
    }

    @Override
    public Result followCommons(Long id) {
        // 获取当前用户
        Long userId = UserHolder.getUser().getId();
        String key = FOLLOW_KEY_PREFIX + userId;
        // 求交集
        String targetKey = FOLLOW_KEY_PREFIX + id;
        Set<String> intersection = stringRedisTemplate.opsForSet().intersect(key, targetKey);
        if (CollUtil.isEmpty(intersection)) {
            // 无交集
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = intersection.stream().map(Long::valueOf).collect(Collectors.toList());
        List<UserDTO> userVoList = userService.listByIds(ids)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userVoList);
    }



}
