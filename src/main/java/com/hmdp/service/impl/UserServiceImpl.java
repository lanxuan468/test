package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.nio.file.CopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static cn.hutool.core.convert.NumberWordFormatter.format;
import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result sendCode(String phone, HttpSession session) {
    //校验手机号。RegexUtils.isPhoneInvalid(phone)，invalid就是不合理的
        if(RegexUtils.isPhoneInvalid(phone)){
            //不正确，fail
            return Result.fail("手机号不合理");
        }
        //正确，RandomUtil.randomNumbers(6)生成验证码。stringRedisTemplate.opsForValue().set保存到redis2分钟。  log.debug日志打印，return返回ok
        String code = RandomUtil.randomNumbers(6);
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY+phone,code, LOGIN_CODE_TTL,TimeUnit.MINUTES);//这里key有规范
        log.debug("发送验证码成功:{}",code);//log.debug加参数要{}
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm) {
    //验证手机号格式
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号不正确");
        }
        String codeLog = loginForm.getCode();
        String codeRedis = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        //从redis提取验证码，从loginForm提取验证码对比
        if(codeRedis==null || !codeRedis.equals(codeLog)){// if(codeLog != codeRedis)
            return Result.fail("验证码错误");
        }
        //成功就判断用户是否存在，不存在在创建一个。存对象进session
        User user = query().eq("phone", phone).one();
        if(user == null){
             user = createUserWithPhone(phone);
        }
        //用
        //用BeanUtil将uesr转为userDTO再转为hash结构。和token存入redis中，返回token给前端
        String token = UUID.fastUUID().toString(true);
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        // 忽略值为null的数据
                        .setIgnoreNullValue(true)
                        // 参数：字段名和字段值，返回值是修改后的字段值。将字段值转为字符串
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        String tokenKey = LOGIN_USER_KEY+token;
        stringRedisTemplate.opsForHash().putAll(tokenKey,userMap);
        stringRedisTemplate.expire(tokenKey,LOGIN_USER_TTL,TimeUnit.MINUTES);
        return Result.ok(token);
    }

    @Override
    public Result sgin() {
//1.获取当前用户
        Long userId = UserHolder.getUser().getId();
//        2.获取当前年月
        String keySuffix = LocalDateTime.now().format(DateTimeFormatter.ofPattern(":yyyy:MM"));
        //3.拼接成key
        String key= USER_SIGN_KEY+userId+keySuffix;
        //4.获取日在月中第几天
        int dayOfMonth = LocalDateTime.now().getDayOfMonth();
        //5.写入redis
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth-1,true);
        return Result.ok();
    }

    @Override
    public Object sginCount() {
        // 获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        // 获取当前日期
        LocalDateTime now = LocalDateTime.now();
        // 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyy:MM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        // 获取今天是本月的第几天（offset）
        int dayOfMonth = now.getDayOfMonth();
        // 获取本月截止今天的所有签到记录，返回一个十进制的数字 BITFIELD key GET u[dayOfMonth] 0
        List<Long> results = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create().get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
        );
        // 没有任何签到结果
        if (CollUtil.isEmpty(results)) {
            return Result.ok(0);
        }
        long num = results.get(0).longValue();
        if (num == 0) return Result.ok(0);
        // 循环遍历，计数统计
        int count = 0;
        while ((num & 1) != 0) {
            // 如果最后一位不为0，说明已签到，计数器+1
            count++;
            // 把数字右移一位，抛弃最后一位，继续下一个位
            num >>>= 1; // 因为java中的整形都是有符号数，>>>是无符号右移，左边最高位补0，如果是>>最高位补符号位，对于正数来说，无符号右移和有符号右移结果相同
        }
        return Result.ok(count);}

    private User createUserWithPhone(String phone) {

        User user=new User();
        user.setPhone(phone);
        user.setNickName("user"+RandomUtil.randomString(10));//Nick“用户”
        save(user);//保存到数据库
        return user;
    }
}
