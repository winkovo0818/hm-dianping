package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
    /**
     * 关注功能
     * @param followUserId 被关注的用户id
     * @param isFollow 关注还是取消关注
     */
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        String key = "follows:"+ userId;
        //判断是关注还是取消关注
        if(isFollow) {
            //关注
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean save = save(follow);
            if (save) {
                stringRedisTemplate.opsForSet().add(key,followUserId.toString());
            }
        }else {
            //取消关注
           remove(new QueryWrapper<Follow>().eq("user_id", userId).eq("follow_user_id",followUserId));
            stringRedisTemplate.opsForSet().remove(key,followUserId.toString());
        }
        return Result.ok();
    }

    /**
     * 判断是否关注
     */
    @Override
    public Result isFollow(Long followUserId) {
        //判断是否关注
        Integer count = query().eq("user_id", UserHolder.getUser().getId()).eq("follow_user_id", followUserId).count();
        return Result.ok(count>0);
    }
    /**
     * 共同关注
     */
    @Override
    public Result commonFollow(Long id) {
        Long currentId = UserHolder.getUser().getId();
        String key = "follows:";
        //求交集
        Set<String> set = stringRedisTemplate.opsForSet().intersect(key + currentId, key + id);
        if (set == null || set.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        //转换成List<Long>
        List<Long> list = set.stream().map(Long::valueOf).collect(Collectors.toList());
        //查询用户信息
        List<UserDTO> users = userService.listByIds(list)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(users);
    }
}

