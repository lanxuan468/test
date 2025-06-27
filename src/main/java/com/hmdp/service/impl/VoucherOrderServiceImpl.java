package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IVoucherService;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT= new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    private static final ExecutorService SECKILL_ORDER_EXECUTOR= Executors.newSingleThreadExecutor();

    /// /用分布式锁
    /// /        SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, "order:"+userId);
    ///
    //RLock lock = redissonClient.getLock("lock:order:" + userId);
    //boolean isLock = lock.tryLock();
    //        if (!isLock) {
    //        return Result.fail("一个用户只能下一单");
    //        }
    //
    //                try {
    //IVoucherService proxy = (IVoucherService) AopContext.currentProxy();
    //            return proxy.createVoucherOrder(voucherId);
    //        } finally {
    //                lock.unlock();
    //        }
/*    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //5一人一单
        Long userId = UserHolder.getUser().getId();


        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherOrder).count();
        if (count > 0) {
            log.error("用户已经购买过一次");
            return;
        }
        //6扣减库存

        boolean success = seckillVoucherService.update()
                .setSql("stock= stock-1")//set stock=stock-1
                .eq("voucher_id", voucherOrder).gt("stock", 0)//where id=? and stock=?
                .update();
        if (!success) {
            //扣减失败
            log.error("库存不足");
            return;
        }

        save(voucherOrder);

    }*/
        private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);

    // Stream消息队列相关属性
    private static final String GROUP_NAME = "g1";    // 消费者组 groupName
    private static final String CONSUMER_NAME = "c1";    // 消费者名称 consumer，该项后期可以在yaml中配置多个消费者，并实现消费者组多消费模式
    private static final String QUEUE_NAME = "stream.orders";    // 消息队列名称 key

    @PostConstruct  // 在类初始化时执行该方法
    private void init() {
        // 创建消息队列
        if (Boolean.FALSE.equals(stringRedisTemplate.hasKey(QUEUE_NAME))) {
            stringRedisTemplate.opsForStream().createGroup(QUEUE_NAME, ReadOffset.from("0"), GROUP_NAME);
            log.debug("Stream队列创建成功");
        }
        // 启动线程池，执行任务
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 从订单信息里获取用户id（从线程池中取出的是一个全新线程，不是主线程，所以不能从BaseContext中获取用户信息）
        Long userId = voucherOrder.getUserId();
        // 创建锁对象(可重入)，指定锁的名称
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁，空参默认失败不等待，失败直接返回
        boolean isLock = lock.tryLock();
        // 获取锁失败，返回错误或重试（这里理论上不需要再做加锁和判断，因为抢单环节的lua脚本已经保证了业务执行的原子性，不允许重复下单）
        if (!isLock) {
            log.error("不允许重复下单，一个人只允许下一单！");
            return;
        }
        try {
            // 将创建的秒杀券订单异步写入数据库
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }
    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while (true) {  // 不断获取消息队列中的订单信息
                try {
                    // 获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, CONSUMER_NAME),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(QUEUE_NAME, ReadOffset.lastConsumed())
                    );
                    // 判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 如果获取失败，说明没有消息，继续下一次读取
                        continue;
                    }
                    // 解析消息中的订单信息 MapRecord<消息id, 消息key，消息value>
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // ACK确认 SACK stream.orders g1 id [id1 id2 id3 ...]
                    stringRedisTemplate.opsForStream().acknowledge(QUEUE_NAME, GROUP_NAME, record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
    }

        }
 /*   private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
    private class VoucherOrderHandler implements Runnable{

        @PostConstruct
        private void init(){
            SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
        }
        @Override
        public void run() {
            while (true){
                try {
                    //1.获取队列中订单消息
                    VoucherOrder voucherOrder = orderTasks.take();
                    //2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                   log.error("处理订单异常",e);
                }

            }
        }
    }*/

    private void handlePendingList() {
        while (true) {  // 不断获取消息队列中的订单信息
            try {
                // 获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from(GROUP_NAME, CONSUMER_NAME),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create(QUEUE_NAME, ReadOffset.from("0"))
                );
                // 判断异常消息获取是否成功
                if (list == null || list.isEmpty()) {
                    // 如果获取失败，说明pending-list中没有异常消息，结束循环
                    break;
                }
                // 解析消息中的订单信息 MapRecord<消息id, 消息key，消息value>
                MapRecord<String, Object, Object> record = list.get(0);
                Map<Object, Object> values = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                // 如果获取成功，可以下单
                handleVoucherOrder(voucherOrder);
                // ACK确认 SACK stream.orders g1 id [id1 id2 id3 ...]
                stringRedisTemplate.opsForStream().acknowledge(QUEUE_NAME, GROUP_NAME, record.getId());
            } catch (Exception e) {
                log.error("处理订单异常", e);
                // 防止处理频繁，下次循环休眠20毫秒
                try {
                    Thread.sleep(20);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }



    private IVoucherService proxy;
      @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
          //2。1从之前写的生成全局唯一id的方法，生成订单id并写入对象的属性
          Long orderId = redisIdWorker.nextId("order");
//        1执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),userId.toString(),String.valueOf(orderId)
        );

//        2判断结果是否为0
        int r = result.intValue();
//        2.1为0，代表没有购买资格
        if(r !=0){
            return Result.fail(r==1 ? "库存不足":"不能重复下单");
        }

        //3获取代理对象
      proxy = (IVoucherService) AopContext.currentProxy();
//        4返回订单id
        return Result.ok(orderId);
    }

    }

  /*  @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
//        1执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),userId.toString()
        );

//        2判断结果是否为0
        int r = result.intValue();
//        2.1为0，代表没有购买资格
        if(r !=0){
            return Result.fail(r==1 ? "库存不足":"不能重复下单");
        }
//        2.2不为0，代表有购买资格
    //2创建订单
    VoucherOrder voucherOrder = new VoucherOrder();
    //2。1从之前写的生成全局唯一id的方法，生成订单id并写入对象的属性
    Long orderId = redisIdWorker.nextId("order");
    voucherOrder.setId(orderId);
    //2.2用户id
    voucherOrder.setUserId(userId);
    //2.3代金卷id
    voucherOrder.setVoucherId(voucherId);
//        7.4放入阻塞队列
        orderTasks.add(voucherOrder);
        //3获取代理对象
      proxy = (IVoucherService) AopContext.currentProxy();
//        4返回订单id
        return Result.ok(orderId);
    }

    }*/
////1查询优惠劵
//SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
////2判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//        //秒杀尚未开始
//        return Result.fail("秒杀尚未开始");
//        }
//                //3判断秒杀是否结束
//                if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//        return Result.fail("秒杀已经结束");
//        }
//                //4判断库存是否充足
//                if (voucher.getStock()<1) {
//        return Result.fail("库存不足");
//        }
//
//Long userId = UserHolder.getUser().getId();
//

//@Transactional
//public  Result createVoucherOrder(Long voucherId) {
//    //5一人一单
//    Long userId = UserHolder.getUser().getId();
//
//
//    Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//    if (count > 0) {
//        return Result.fail("用户已经购买过一次");
//    }
//    //6扣减库存
//
//    boolean success = seckillVoucherService.update()
//            .setSql("stock= stock-1")//set stock=stock-1
//            .eq("voucher_id", voucherId).gt("stock", 0)//where id=? and stock=?
//            .update();
//    if (!success) {
//        //扣减失败
//        return Result.fail("库存不足");
//    }
//
//    //7创建订单
//
//    VoucherOrder voucherOrder = new VoucherOrder();
//    //7。1从之前写的生成全局唯一id的方法，生成订单id并写入对象的属性
//    Long orderId = redisIdWorker.nextId("order");
//    voucherOrder.setId(orderId);
//    //7.2用户id
//    voucherOrder.setUserId(userId);
//    //7.3代金卷id
//    voucherOrder.setVoucherId(voucherId);
//    save(voucherOrder);
//    //返回对象
//
//    return Result.ok(orderId);
//
//}

