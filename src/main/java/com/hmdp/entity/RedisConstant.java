package com.hmdp.entity;

public interface RedisConstant {
    String SHOP_CACHE = "cache:shop::";
    Long CACHE_NULL_TTL = 2L;
    long CACHE_SHOP_TTL = 30L;
    long LOCK_SHOP_TTL = 10L;
    String SHOP_LOCK = "lock:shop::";

}
