package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.RedisConstant;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
@Slf4j
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;
    private static final ExecutorService CACHE_REBUILD_POOL = Executors.newFixedThreadPool(10);
    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    //设置缓存
    public void set(String key, Object value, Long time, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,timeUnit);
    }
    // 设置缓存 逻辑过期
    private void setLogical(String key,Object value,Long time,TimeUnit timeUnit){
        RedisData data = new RedisData();
        data.setData(value);
        data.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value));
    }
    private boolean tryLock(String key){
        Boolean b = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstant.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(b);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    /**
     * 封装缓存穿透查询函数
     * @param keyPrefix 缓存key前缀 例如 cache:shop::
     * @param id 查询id 例如 shopId
     * @param clazz 返回类型 例如 Shop.class
     * @param function 查询数据库函数 例如 id -> shopMapper.selectById(id)
     */
    public <R,ID> R queryWithThrough(
            String keyPrefix, ID id, Class<R> clazz, Function<ID,R> function,
            Long time,TimeUnit timeUnit){
        String key = keyPrefix + id;
        // 先从缓存中查询 cache:shop::
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, clazz);
        }
        //如果返回的是空字符串 ""
        if (json!=null){
            //返回错误信息
            return null;
        }
        //如果缓存中没有,再从数据库中查询
        R r = function.apply(id);
        if (r == null) {
            //将空值写入缓存
            stringRedisTemplate.opsForValue().set(key, "", RedisConstant.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //写入缓存
        this.set(key, r, time, timeUnit);
        return r;
    }

    /**
     * 封装缓存击穿查询函数
     * @param keyPrefix 缓存key前缀 例如 cache:shop::
     * @param id 查询id
     * @param clazz 返回类型
     * @param function 查询数据库函数 例如 id -> shopMapper.selectById(id)
     * @param time 过期时间
     * @param timeUnit 时间单位
     */
    public <R,ID> R queryWithLogicalExpire(String keyPrefix,ID id,Class<R> clazz,Function<ID,R> function,Long time,TimeUnit timeUnit){
        String key = keyPrefix + id;
        //先从缓存中查询
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(json)) {
            return null;
        }
        //如果存在 判断是否过期
        RedisData data = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean(JSONUtil.toJsonStr(data.getData()), clazz);
        LocalDateTime expireTime = data.getExpireTime();
        // 如果未过期 直接返回
        if (expireTime.isAfter(LocalDateTime.now())) {
            return r;
        }
        // 如果过期 利用互斥锁防止缓存击穿
        String lockKey = RedisConstant.SHOP_LOCK + id;
        boolean lock = tryLock(lockKey);
        //如果获取锁成功 开启独立线程异步更新缓存
        if (lock) {
            CACHE_REBUILD_POOL.submit(()->{
                try {
                    //查询数据库
                    R r1 = function.apply(id);
                    this.setLogical(key, r1, time, timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        return JSONUtil.toBean(json, clazz);
    }
}
