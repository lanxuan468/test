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
      //参数：followId和
        //返回ok就行
        //获取当前userId，因为字段封装成了对象，所以就是赋值后保存对象即可
         //获取id，当前关注了->根据userid和followid删除记录；没关注->生成一个对象，保存
        Long userId = UserHolder.getUser().getId();
        String key=FOLLOW_KEY_PREFIX+userId;
        if(!isFollow){
        remove(new QueryWrapper<Follow>()
                    .eq("userId", userId)
                    .eq("followId", followId));
        }
      else{
            Follow follow = new Follow().setFollowUserId(followId).setUserId(userId);
            save(follow);
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
            List<Long> ids = this.baseMapper.commentId(userId,id);
            if(CollUtil.isEmpty(ids)){
                return Result.ok(Collections.emptyList());
            }
            List<UserDTO> collect = userService.listByIds(ids)
                    .stream()
                    .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                    .collect(Collectors.toList());
            return Result.ok(collect);
    }



}
