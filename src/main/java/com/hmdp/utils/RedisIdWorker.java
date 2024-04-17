package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    private static final int COUNT_BITS = 32;
    private static final long BEGIN_TIME = 1704067200L;
    private final StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    public long nextId(String keyPrefix){
        LocalDateTime now = LocalDateTime.now();
        long timestamp = now.toEpochSecond(ZoneOffset.UTC);
        //时间戳-开始时间戳
        long offset = timestamp - BEGIN_TIME;
        //获取当前日期 精确到天
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //获取当天的自增序列
        long count = stringRedisTemplate.opsForValue().increment("incr:" + keyPrefix + ":" + date);
        //拼接id
        return offset<<COUNT_BITS | count;
    }
}
