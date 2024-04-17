package com.hmdp.utils;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class RedisLock implements ILock{
    private final String name;
    private final StringRedisTemplate stringRedisTemplate;
    public RedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }
    private static final String keyPrefix = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID()+"-";
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    //静态代码块，初始化解锁脚本 每次调用都会初始化一次 自动加载lua脚本
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        String id = ID_PREFIX + Thread.currentThread().getId();
        //尝试获取锁
        Boolean result = stringRedisTemplate.opsForValue().setIfAbsent(keyPrefix + name, id, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(result);
    }

    /**
     * 释放锁
     * 这里使用了lua脚本来保证原子性
     * Collections.singletonList(keyPrefix + name) 传入的是key的集合
     * ID_PREFIX + Thread.currentThread().getId() 传入的是参数
     */
    @Override
    public void unlock() {
        //调用lua脚本
        stringRedisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(keyPrefix + name),
                ID_PREFIX + Thread.currentThread().getId());
    }
}
