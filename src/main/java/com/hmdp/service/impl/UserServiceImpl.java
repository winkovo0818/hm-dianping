package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

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
    private RedisTemplate<String,Object> redisTemplate;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result sendCode(String phone, HttpSession session) {
        //校验手机号
        boolean result = RegexUtils.isPhoneInvalid(phone);
        if (result) {
            return Result.fail("手机号格式不正确");
        }
        String code = RandomUtil.randomNumbers(6);
        //生成验证码 保存到redis 有效期5分钟 redisson
        redisTemplate.opsForValue().set("login:code:"+phone, code, 2, java.util.concurrent.TimeUnit.MINUTES);
        log.debug("验证码为:{}", code);
        return Result.ok();
    }

    /**
     * 用户签到
     */
    @Override
    public Result sign() {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("用户未登录");
        }
        //获取日期
        LocalDateTime now = LocalDateTime.now();
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = "sign:"+user.getId()+keySuffix;
        //获取今天是本月的第几天
        long day = now.getDayOfMonth();
        Boolean succeed = stringRedisTemplate.opsForValue().getBit(key, day - 1);
        if (Boolean.TRUE.equals(succeed)) {
            return Result.fail("今天已经签到过了");
        }
        stringRedisTemplate.opsForValue().setBit(key, day-1, true);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        if (RegexUtils.isPhoneInvalid(loginForm.getPhone())) {
            return Result.fail("手机号格式不正确");
        }
        //校验验证码
        Object code = redisTemplate.opsForValue().get("login:code:"+loginForm.getPhone());
        if (code == null || !code.equals(loginForm.getCode())) {
            return Result.fail("验证码错误");
        }
        //校验通过，查询用户信息
        User user = this.baseMapper.selectOne(new QueryWrapper<User>().eq("phone", loginForm.getPhone()));
        if (user == null) {
            //用户不存在，自动注册
            user = this.createUserByPhone(loginForm.getPhone());
        }
        //将用户信息保存到redis 利用hash结构存储
        String token = UUID.randomUUID().toString();
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        //使用RedisTemplate
        redisTemplate.opsForHash().putAll("login:token:"+token, BeanUtil.beanToMap(userDTO));
        return Result.ok(token);
    }


    @Override
    public User createUserByPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName("user_"+RandomUtil.randomString(6));
        this.save(user);
        return user;
    }

    /**
     * 查询用户签到次数
     */
    @Override
    public Result signCount() {
        Long id = UserHolder.getUser().getId();
        LocalDateTime now = LocalDateTime.now();
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = "sign:"+id+keySuffix;
        int day = now.getDayOfMonth();
        //获取本月截止到今天的所有签到记录
        //BITFIELD key GET u10 0
        List<Long> result = stringRedisTemplate.opsForValue().bitField(key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(day)).valueAt(0));
        if (result==null||result.isEmpty()) {
            //没有签到记录
            return Result.ok(0);
        }
        //这里的num是一个十进制数，代表了签到的次数
        Long num = result.get(0);
        if (num == null || num == 0) {
            return Result.ok(0);
        }
        //循环遍历num的每一位，计算签到次数
        int count = 0;
        while (((num & 1) != 0)) {
            count++;
            num >>>= 1;
        }
        return Result.ok(count);
    }

}

