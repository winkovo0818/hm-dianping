package com.hmdp;

import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.service.impl.UserServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@SpringBootTest
@Slf4j
class HmDianPingApplicationTests {
    @Resource
    private UserServiceImpl userService;
    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private RedisTemplate<String,Object> redisTemplate;
    @Resource
    private UserMapper userMapper;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Test
    void test() {
        //获取全部用户
        List<User> users = userMapper.selectList(null);
        for (User user : users) {
            Result result = userService.sendCode(user.getPhone(), null);
            //获取验证码
            String code = (String) redisTemplate.opsForValue().get("login:code:"+user.getPhone());
            //登录
            LoginFormDTO loginForm = new LoginFormDTO();
            loginForm.setPhone(user.getPhone());
            loginForm.setCode(code);
            Result loginResult = userService.login(loginForm, null);
        }

            //读取Redis中所有的key
            Set<String> keys = redisTemplate.keys("*");
            //将keys分割 login:token:1 -> login token 1
            List<String> lines = new ArrayList<>();
            if (keys != null) {
                keys.forEach(key -> {
                    String[] split = key.split(":");
                    // Only add the last element of split to lines
                    lines.add(split[split.length - 1]);
                });
                String filePath = "D:\\Code\\Code\\JavaCode\\hm-dianping\\src\\main\\resources\\tokens.txt";
                try {
                    // Write lines to file
                    Files.write(Paths.get(filePath), lines, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
    }

    @Test
    void loadShopData() {
        List<Shop> list = shopService.list();
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>();
        //将数据写入Redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            Long typeId = entry.getKey();
            List<Shop> value = entry.getValue();
            String key = "shop:geo:" + typeId;
            value.forEach(shop -> {
//              stringRedisTemplate.opsForGeo().add(key,new Point(shop.getX(),shop.getY()),shop.getId().toString());
                //将商铺信息写入Redis
                locations.add(
                        new RedisGeoCommands.GeoLocation<>(
                                shop.getId().toString(),
                                new Point(shop.getX(),shop.getY())
                        ));
            });
            //一次性写入 效率更高
            stringRedisTemplate.opsForGeo().add(key,locations);
        }
    }

    @Test
    void testHyperLogLog() {
        String [] users =  new String[1000];
        int index = 0;
        for (int i = 1; i < 10000; i++) {
            users[index++] = "user_"+i;
            if (i%1000==0){
                index = 0;
                stringRedisTemplate.opsForHyperLogLog().add("hll1",users);
            }
        }
        Long size = stringRedisTemplate.opsForHyperLogLog().size("hll1");
        log.info("size:{}",size);
    }
}
