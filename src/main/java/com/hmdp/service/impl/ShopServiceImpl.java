package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.RedisConstant;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;
    // 缓存重建线程池
    private static final ExecutorService CACHE_REBUILD_POOL = Executors.newFixedThreadPool(10);

    /**
     * 将热点数据写入redis 其中expireTime为逻辑过期时间
     */
    public void writeDataToRedis(Long id, Long expireTime){
        Shop shop = getById(id);
        RedisData data = new RedisData();
        data.setData(shop);
        data.setExpireTime(LocalDateTime.now().plusSeconds(expireTime));
        stringRedisTemplate.opsForValue().set(RedisConstant.SHOP_CACHE + id, JSONUtil.toJsonStr(data));
    }

    /**
     * 更新商铺信息
     * 先更新数据库，再删除缓存 防止脏数据
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public Result update(Shop shop) {
        if (shop.getId() == null || shop.getId() <= 0) {
            return Result.fail("商铺id不能为空");
        }
        updateById(shop);
        stringRedisTemplate.delete(RedisConstant.SHOP_CACHE + shop.getId());
        return Result.ok();
    }

    @Override
    public Result queryByType(Integer typeId, Integer current, Double x, Double y){
        // 如果x和y为空 则根据类型分页查询
        if (x == null || y == null) {
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        String key = "shop:geo:" + typeId;
        //计算分页参数
        int start = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = SystemConstants.DEFAULT_PAGE_SIZE * current;
        // GEOSEARCH KEY BYLONLAT x y BYRADIUS 10 WITHDISSATNCE
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs()
                                .includeDistance()
                                .limit(end)
                );
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size()<=start){
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = new ArrayList<>(list.size());
        Map<Long,Distance> map = new HashMap<>(list.size());
        //截取分页数据
        list.stream().skip(start).forEach(result->{
            String shopId = result.getContent().getName();
            Distance distance = result.getDistance();
            ids.add(Long.valueOf(shopId));
            map.put(Long.valueOf(shopId),distance);
        });
        String join = StrUtil.join(",", ids);
        //根据商铺id查询商铺信息
        List<Shop> shops = query().in("id", ids)
                .last("Order by field(id," + join + ")")
                .list();
        for (Shop shop : shops) {
            Distance distance = map.get(shop.getId());
            shop.setDistance(distance.getValue());
        }
        return Result.ok(shops);
    }


    /**
     * 缓存击穿解决方案
     */
    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //todo Shop shop = queryWithThrough(id);
        //缓存击穿 利用互斥锁解决
        // Shop shop = queryWithBreakdown(id);
        // 缓存击穿解决方案 利用逻辑过期时间
        //Shop shop = queryWithLogicalExpire(id);
        if (id<=0) {
            return Result.fail("商铺id不能为空");
        }
        Shop shop = cacheClient.queryWithThrough(RedisConstant.SHOP_CACHE, id, Shop.class, this::getById, RedisConstant.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //Shop = cacheClient.queryWithLogicalExpire(RedisConstant.SHOP_CACHE + id, id, Shop.class, this::getById, RedisConstant.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (shop == null) {
            return Result.fail("商铺不存在");
        }
        //互斥锁解决缓存击穿
        return Result.ok(shop);
    }

    /**
     * 缓存穿透解决方案
     */
    @Override
    public Shop queryWithThrough(Long id) {
        // 先从缓存中查询 cache:shop::
        String s = stringRedisTemplate.opsForValue().get(RedisConstant.SHOP_CACHE + id);
        if (StrUtil.isNotBlank(s)) {
            return JSONUtil.toBean(s, Shop.class);
        }
        //如果返回的是空字符串 ""
        if (s!=null){
            //返回错误信息
            return null;
        }
        //如果缓存中没有,再从数据库中查询
        Shop shop = this.getById(id);
        if (shop == null) {
            //将空值写入缓存
            stringRedisTemplate.opsForValue().set(RedisConstant.SHOP_CACHE + id, "", RedisConstant.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //写入缓存
        stringRedisTemplate.opsForValue().set(RedisConstant.SHOP_CACHE + id, JSONUtil.toJsonStr(shop), RedisConstant.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    private boolean tryLock(String key){
        Boolean b = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstant.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(b);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    //  互斥锁解决缓存击穿
    public Shop queryWithBreakdown(Long id){
        //先判断是否为异常值
        String key = RedisConstant.SHOP_CACHE + id;
        //先从缓存中查询
        String s = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(s)) {
            return JSONUtil.toBean(s, Shop.class);
        }
        //如果返回的是空字符串 ""
        if (s!=null){
            //返回错误信息
            return null;
        }
        //创建互斥锁 "lock:shop::" + id
        String lockKey = RedisConstant.SHOP_LOCK + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            if(!isLock){
                //失败则休眠一段时间后重试
                Thread.sleep(50);
                return queryWithBreakdown(id);
            }
            //获取锁成功查询数据库
            shop = this.getById(id);
            //模拟高并发场景
            Thread.sleep(200);
            if (shop == null) {
                //将空值写入缓存
                stringRedisTemplate.opsForValue().set(key, "", RedisConstant.CACHE_NULL_TTL, TimeUnit.MINUTES);
                unlock(lockKey);
                return null;
            }
            //写入缓存
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstant.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unlock(lockKey);
        }
        return shop;
    }

    /**
     * 缓存击穿解决方案
     * 利用逻辑过期时间
     */
    private Shop queryWithLogicalExpire(Long id) {
        //先从缓存中查询
        String s = stringRedisTemplate.opsForValue().get(RedisConstant.SHOP_CACHE + id);
        if (StrUtil.isBlank(s)) {
            return null;
        }
        //如果存在 判断是否过期
        RedisData data = JSONUtil.toBean(s, RedisData.class);
        // 如果未过期 直接返回
        if (data.getExpireTime().isAfter(LocalDateTime.now())) {
            return (Shop) data.getData();
        }
        // 如果过期 利用互斥锁防止缓存击穿
        String lockKey = RedisConstant.SHOP_LOCK + id;
        boolean lock = tryLock(lockKey);
        //如果获取锁成功 开启独立线程异步更新缓存
        if (lock) {
            CACHE_REBUILD_POOL.submit(()->{
                try {
                    this.writeDataToRedis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        return JSONUtil.toBean(s, Shop.class);
    }
}
