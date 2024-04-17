package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;
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
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private UserServiceImpl userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private FollowServiceImpl followerService;
    @Override
    public Result queryBlogById(long id) {
        // 查询博文
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("博文不存在");
        }
        // 查询用户
        queryBlogUser(blog);
        isLiked(blog);
        return Result.ok(blog);
    }

    private void queryBlogUser(Blog blog) {
        User user = userService.getById(blog.getUserId());
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            queryBlogUser(blog);
            isLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result likeBlog(Long id) {
        Long userId = UserHolder.getUser().getId();
        if (id<=0){
            return Result.fail("非法参数");
        }
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("博文不存在");
        }
        String key = "blog:like:" + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        //如果没有点赞过
        if (score == null) {
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if (isSuccess) {
                //将用户id加入到集合中 传入当前时间戳判断点赞顺序
                stringRedisTemplate.opsForZSet().add(key, userId.toString(),System.currentTimeMillis());
            }
        }
        else{
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        //查询top5的点赞用户 zrange key start stop
        String key = "blog:like:" + id;
        Set<String> userIds = stringRedisTemplate.opsForZSet().reverseRange(key, 0, 4);
        if (userIds != null && userIds.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = null;
        if (userIds != null) {
            ids = userIds.stream().map(Long::valueOf).collect(Collectors.toList());
        }
        String join = StrUtil.join(",", ids);
        //这里必须要指定排序 否则会导致数据顺序错乱
        List<UserDTO> users = userService.query().in("id", ids)
                .last("Order by field(id," + join + ")")
                .list()
                .stream()
                .map(user -> BeanUtil.toBean(user, UserDTO.class)).collect(Collectors.toList());
        return Result.ok(users);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean save = save(blog);
        if (!save) {
            return Result.fail("新增笔记失败");
        }
        //查询当前用户的粉丝
        List<Follow> follows = followerService.query().eq("follow_user_id", user.getId()).list();
        for (Follow follow : follows) {
            //将博文id加入到粉丝的收件箱中 Zset存储
            Long userId = follow.getUserId();
            //推送到粉丝的feed中
            String key = "feed:" + userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        // 返回id
        return Result.ok(blog.getId());
    }

    /**
     * 查询关注的博文
     */
    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        // 查询收件箱 ZREVANGE key MAX MIN LIMIT offset count
        String key = "feed:" + user.getId();
        //解析数据
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, SystemConstants.MAX_PAGE_SIZE);
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        //解析数据
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int i = 1;
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            //将博文id加入到集合中
            ids.add(Long.valueOf(Objects.requireNonNull(tuple.getValue())));
            //获取分数 最小时间戳
            long time = Objects.requireNonNull(tuple.getScore()).longValue();
            //如果当前时间戳和上一个时间戳相同 则页码+1
            if (time == minTime) {
                i++;
            } else {
                minTime = time;
                i = 1;
            }
        }
        String join = StrUtil.join(",", ids);
        //根据id查询博文
        List<Blog> blogList = query().in("id", ids)
                .last("Order by field(id," + join + ")")
                .list();
        blogList.forEach(blog -> {
            queryBlogUser(blog);
            isLiked(blog);
        });
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogList);
        scrollResult.setMinTime(minTime);
        scrollResult.setOffset(i);
        return Result.ok(scrollResult);
    }

    public void isLiked(Blog blog){
        UserDTO userDTO = UserHolder.getUser();
        if (userDTO == null) {
            return;
        }
        Long userId = userDTO.getId();
        String key = "blog:like:" + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }
}
