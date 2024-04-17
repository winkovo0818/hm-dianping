package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public List<ShopType> queryList() {
        // 先从缓存中查询
        String s = stringRedisTemplate.opsForValue().get("shop-type::list");
        if (s != null) {
            return JSONUtil.toList(s, ShopType.class);
        }
        //如果缓存中没有，再从数据库中查询
        List <ShopType> list = this.list();
        if (list != null) {
            // 写入缓存
            stringRedisTemplate.opsForValue().set("shop-type::list", JSONUtil.toJsonStr(list));
        }
        return list;
    }
}
