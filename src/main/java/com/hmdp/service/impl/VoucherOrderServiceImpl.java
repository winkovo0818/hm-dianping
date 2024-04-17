package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redisClient;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setResultType(Long.class);
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
    }
    /**
     * 初始化方法
     * 只要容器初始化了这个类，就会执行这个方法
     */
    //线程池 这里用的是Executors.newSingleThreadExecutor() 单线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandle());
    }

    private class VoucherOrderHandle implements Runnable{
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    //从消息队列中取出订单信息 XREADGROUP GROUP g1 c1 COUNT 1 Block 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed()));
                    if (list == null || list.isEmpty()) {
                        continue;
                    }
                    //订单获取成功 解析订单信息
                    MapRecord<String, Object, Object> entries;
                    entries = list.get(0);
                    Map<Object, Object> value = entries.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    //确认消息
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", entries.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    break;
                    //handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while(true){
                try {
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    if (list == null || list.isEmpty()) {
                        //如果获取失败
                        break;
                    }
                    //订单获取成功 解析订单信息
                    MapRecord<String, Object, Object> entries = list.get(0);
                    Map<Object, Object> values = entries.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    //确认消息
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", entries.getId());
                }
                catch (Exception e) {
                    log.error("处理pendingList异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }


    /**
     * 处理订单 从阻塞队列中取出订单信息 生成订单
     */
    //阻塞队列
//    private final BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);
//    private class VoucherOrderHandle implements Runnable{
//        @Override
//        public void run() {
//            //优化思路 只有队列中有订单才处理
//            //while(!orderTasks.isEmpty())
//            while (true) {
//                try {
//                    VoucherOrder order = orderTasks.take();
//                    handleVoucherOrder(order);
//                } catch (Exception e) {
//                   log.error("处理订单异常",e);
//                }
//            }
//        }
//    }
    private void handleVoucherOrder(VoucherOrder order) {
        Long userId = order.getUserId();
        RLock lock = redisClient.getLock("lock:order:" + userId);
        //尝试获取锁
        boolean locked = lock.tryLock();
        if (!locked) {
            log.error("获取锁失败");
            return;
        }
        try {
            proxy.createOrder(order);
        }finally {
            lock.unlock();
        }

    }
    /**
     * 秒杀业务
     * 在这里执行lua脚本
     * 创建一个代理对象 用于执行createOrder方法
     */
    private IVoucherOrderService proxy;
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //获取订单id
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );
        //2.判断结果是为0
        assert result != null;
        int r = result.intValue();
        if ((r != 0)) {
            //2.1 不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 3.获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //3.返还订单id
        return Result.ok(orderId);
    }
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        Long userId = UserHolder.getUser().getId();
//        //执行lua脚本
//        Long executed = stringRedisTemplate.execute(SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(),
//                userId.toString());
//        //判断返回结果 1库存不足 2重复下单
//        int result = 1;
//        if (executed != null) {
//            result = executed.intValue();
//        }
//        if (result != 0) {
//            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
//        }
//        //程序走到这里说明可以下单
//        long orderId = redisIdWorker.nextId("order");
//        //生成订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        voucherOrder.setId(orderId);
//        voucherOrder.setVoucherId(voucherId);
//        voucherOrder.setUserId(userId);
//
//        //创建一个阻塞队列将订单信息放到队列中
//        orderTasks.add(voucherOrder);
//        IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//        return Result.ok(orderId);
//    }

    /**
     * 创建订单
     */
    @Transactional
    public void createOrder(VoucherOrder order) {
            Long userId = order.getUserId();
            //判断是否已经购买过
            int count = query().eq("voucher_id", order.getVoucherId())
                    .eq("user_id", userId)
                    .count();
            if (count > 0) {
                log.error("重复下单");
            }
            //如果没有订单，减库存
            //这里stoke>0是为了防止超卖
            boolean update = seckillVoucherService.update().setSql("stock = stock - 1")
                    .eq("voucher_id", order.getVoucherId())
                    .gt("stock", 0)
                    .update();
            if (!update) {
                log.error("库存不足");
            }
            save(order);
    }
}
