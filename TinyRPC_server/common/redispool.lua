local skynet = require "skynet"
require "skynet.manager"
local redis = require "db.myredis"

local maxconn, host, port, dbindex, pwd

local CMD = {}

local __batchId = 0
local __batches = {}
local function gen_batch_id()
    local batchId = __batchId + 1
    __batchId = batchId
    return batchId
end

---@type RedisClientList
local redisClientList

---@class RedisClientListNode
local RedisClientListNode = class()
function RedisClientListNode:ctor(redisClient)
    self.redisClient = redisClient
    self.prev = nil
    self.next = nil
end

---@class RedisClientList
local RedisClientList = class()
function RedisClientList:ctor()
    self.head = nil
    self.cur = nil
    self.len = 0
end

function RedisClientList:add(redisClientListNode)
    if self.head then
        local beforeNode = self.head
        redisClientListNode.prev = beforeNode.prev
        beforeNode.prev.next = redisClientListNode
        redisClientListNode.next = beforeNode
        beforeNode.prev = redisClientListNode
    else
        redisClientListNode.prev = redisClientListNode
        redisClientListNode.next = redisClientListNode
        self.head = redisClientListNode
        self.cur = redisClientListNode
    end
    self.len = self.len + 1
end

function RedisClientList:remove(redisClientListNode)
    if redisClientListNode.next == redisClientListNode then
        self.head = nil
        self.cur = nil
    else
        if self.head == redisClientListNode then
            self.head = redisClientListNode.next
        end
        if self.cur == redisClientListNode then
            self.cur = redisClientListNode.next
        end
        redisClientListNode.next.prev = redisClientListNode.prev
        redisClientListNode.prev.next = redisClientListNode.next
    end
    redisClientListNode.prev = nil
    redisClientListNode.next = nil
    self.len = self.len - 1
end

function RedisClientList:peek()
    local cur = self.cur
    if self.cur then
        self.cur = self.cur.next
    end
    return cur and cur.redisClient or nil
end

function RedisClientList:destroy()
    while self.head do
        local redisClientListNode = self.head
        self:remove(redisClientListNode)
        redisClientListNode.redisClient:disconnect()
    end
end


--region Pipeline Mode
function CMD.PIPELINE_START()
    local redisClient = redisClientList:peek()
    if not redisClient then
        return false, "no any available redisClients"
    end

    local batchId = gen_batch_id()
    local buffer = redisClient:new_command_buffer()
    __batches[batchId] = {
        client = redisClient,
        buffer = buffer
    }
    return true, batchId
end

function CMD.PIPELINE_APPEND(batchId, cmd, ...)
    local pipeline = __batches[batchId]
    if not pipeline then
        return false, "invalid batchId"
    end

    local redisClient = pipeline.client
    return redisClient:buffer_command(pipeline.buffer, cmd, ...)
end

function CMD.PIPELINE_CANCEL(batchId)
    __batches[batchId] = nil
end

function CMD.PIPELINE_EXECUTE(batchId)
    local pipeline = __batches[batchId]
    if not pipeline then
        return false, "invalid batchId"
    end
    __batches[batchId] = nil

    local redisClient = pipeline.client
    return redisClient:pipeline_execute(pipeline.buffer)
end
--endregion


--region Transaction Mode
function CMD.TRANSACTION_START()
    local redisClient = redisClientList:peek()
    if not redisClient then
        return false, "no any available redisClients"
    end

    local batchId = gen_batch_id()
    local buffer = redisClient:new_command_buffer()
    __batches[batchId] = {
        client = redisClient,
        buffer = buffer
    }
    redisClient:transaction_command(buffer, "MULTI")
    return true, batchId
end

function CMD.TRANSACTION_APPEND(batchId, cmd, ...)
    local transaction = __batches[batchId]
    if not transaction then
        return false, "invalid batchId"
    end

    local redisClient = transaction.client
    return redisClient:buffer_command(transaction.buffer, cmd, ...)
end

function CMD.TRANSACTION_CANCEL(batchId)
    __batches[batchId] = nil
end

function CMD.TRANSACTION_EXECUTE(batchId)
    local transaction = __batches[batchId]
    if not transaction then
        return false, "invalid batchId"
    end
    __batches[batchId] = nil

    local redisClient = transaction.client
    redisClient:transaction_command(transaction.buffer, "EXEC")
    return redisClient:transaction_execute(transaction.buffer)
end
--endregion


--region Command Mode
function CMD.COMMAND_EXECUTE(cmd, ...)
    local redisClient = redisClientList:peek()
    if not redisClient then
        return false, "no any available redisClients"
    end

    return redisClient:command_execute(cmd, ...)
end
--endregion


function CMD.new_connection()
    local redisClient = redis.RedisClient.new {
        host     = host,
        port     = port,
        dbindex  = dbindex,
        password = pwd
    }

    local redisClientListNode = RedisClientListNode.new(redisClient)
    redisClientList:add(redisClientListNode)
end

function CMD.start(config)
    assert(not table.empty(config))
    maxconn = math.tointeger(config.maxconn) or 10
    host    = assert(config.host)
    port    = assert(math.tointeger(config.port))
    dbindex = assert(math.tointeger(config.dbindex))
    pwd     = assert(config.pwd)
    if maxconn <= 0 then
        maxconn = 10
    end

    LOG_INFO("redispool:CMD:start", {maxconn=maxconn, host=host, port=port, dbindex=dbindex})

    redisClientList = RedisClientList.new()
    for _ = 1, maxconn do
        CMD.new_connection()
    end

    if config.servicename then
        skynet.register("." .. config.servicename)
    end
end

function CMD.stop()
    redisClientList:destroy()
end

function CMD.exit()
    skynet.exit()
end

skynet.start(function()
    skynet.dispatch("lua", function(session, source, cmd, ...)
        local f = assert(CMD[cmd], cmd .. " not found")
        skynet.retpack(f(...))
    end)
end)