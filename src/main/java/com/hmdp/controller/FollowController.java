package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IFollowService;
import com.hmdp.utils.UserHolder;
import net.bytebuddy.implementation.bytecode.constant.DefaultValue;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/follow")
public class FollowController {
    @Resource
    private IFollowService followService;
    @PutMapping("/{id}/{isFollow}")
    public Result isFollow(@PathVariable("id") Long followId,@PathVariable("isFollow") boolean isFollow){
        return followService.setFollow(followId,isFollow);
    }
    @GetMapping("/or/not/{id}")
    public Result isFollow(@PathVariable("id") Long followUserId) {
        return followService.isFollow(followUserId);
    }
    @GetMapping("/common/{id}")
    public Result followCommons(@PathVariable("id") Long id){
        //参数：博主的id。 返回值：list的共同好友
        //有当前用户信息->查自己关注了哪些。  查博主关注了哪些，求交集返回.  关注的时候，在mysql设置的是一个个关系，怎么求？
        //where userid= id得被1关注的id  + 在这些id中where再查被 博主关注的id

        //select id from table where userId=blogid and followId in(select id from table where userId=userid )
//        Long userId = UserHolder.getUser().getId();

        return followService.followCommons(id);
    }

}
