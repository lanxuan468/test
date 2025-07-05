package com.hmdp.mapper;

import com.hmdp.entity.Follow;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface FollowMapper extends BaseMapper<Follow> {

    @Select("select follow_user_id from tb_follow where user_id=#{id} " +
                "and follow_user_id in(select follow_user_id from tb_follow where user_id=#{userId})")
    List<Long> commentId(Long userId, Long id);
}
