--编写lua脚本步骤

--1参数列表
--1.1id
local voucherId =ARGV[1]
local userId=ARGV[2]
local orderId=ARGV[3]

local stockKey='seckill:stock:' .. voucherId;
local orderKey='seckill:stock:' .. voucherId;

--脚本业务
if(tonumber(redis.call('get',stockKey))<=0)then
    return 1
end

if(redis.call('sismember',orderKey,userId)==1)then
    return 2
end

redis.call('incrby',stockKey,-1)
redis.call('sadd',orderKey,userId)
--发送消息到队列
redis.call('xadd','stream.orders','*','userId',userId,'voucherId',voucherId,'id',orderId)
return 0