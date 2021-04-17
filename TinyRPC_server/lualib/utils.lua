local skynet = require "skynet"
local protobuf = require "protobuf"
local json = require "json"

protobuf.register_file("./proto/message.pb")


-------------------
-- TRACEBACK相关start

function __TRACEBACK__(errmsg)
    local traceMsg = debug.traceback(errmsg)
    LOG_FATAL(traceMsg)
    return false
end

-- TRACEBACK相关end
-------------------


-------------------
-- LOG相关start

local function formatLog(logLevel, debugInfo, msg, params)
    local logTable = {
        level   = logLevel,
        time    = os.date("%Y-%m-%d %H:%M:%S"),
        service = SERVICE_NAME
    }

    if debugInfo then
        logTable["debug"] = {
            short_src   = debugInfo.short_src,
            currentline = debugInfo.currentline
        }
    end

    if type(msg) == "string" then
        logTable["msg"] = msg
    else
        logTable["msg"] = "msg error, should be a string"
    end

    if not table.empty(params) then
        logTable["params"] = params
    end

    local success, logContent = pcall(json.stringify, logTable)
    if success then
        return logContent
    else
        return "utils:formatLog, json stringify failed"
    end

    return json.stringify(logTable)
end

function LOG_DEBUG(msg, params)
    local info = debug.getinfo(2, "Sl")
    skynet.error(formatLog("DEBUG", info, msg, params))
end

function LOG_INFO(msg, params)
    local info = debug.getinfo(2, "Sl")
    skynet.error(formatLog("INFO", info, msg, params))
end

function LOG_WARNING(msg, params)
    local info = debug.getinfo(2, "Sl")
    skynet.error(formatLog("WARNING", info, msg, params))
end

function LOG_ERROR(msg, params)
    local info = debug.getinfo(2, "Sl")
    skynet.error(formatLog("ERROR", info, msg, params))
end

function LOG_FATAL(msg, params)
    local info = debug.getinfo(2, "Sl")
    skynet.error(formatLog("FATAL", info, msg, params))
end

-- LOG相关end
-------------------


-------------------
-- PROTOBUF相关start

function message_pb_encode(msg)
    return protobuf.encode("NinjaMessage.Message", msg)
end

function message_pb_decode(data)
    return protobuf.decode("NinjaMessage.Message", data)
end

-- PROTOBUF相关end
-------------------


-------------------
-- MongoDB相关start

function make_temporary_mongo_pool(maxConnections)
    local mongopool = skynet.newservice("mongopool")
    local address = assert(skynet.getenv("mongo_address"))
    maxConnections = math.tointeger(maxConnections)
    if not maxConnections then
        local _, maxconn = string.gsub(address, "[^:,]+:[^,]+", "")
        maxConnections = maxconn * 2
    end
    skynet.call(mongopool, "lua", "start", {
        maxconn = maxConnections,
        address = address,
        authdb  = assert(skynet.getenv("mongo_authdb")),
        db      = assert(skynet.getenv("mongo_db")),
        user    = assert(skynet.getenv("mongo_user")),
        pwd     = assert(skynet.getenv("mongo_pwd"))
    })
    return mongopool
end

function free_temporary_mongo_pool(mongopool)
    if mongopool then
        skynet.call(mongopool, "lua", "stop")
        skynet.send(mongopool, "lua", "exit")
    end
end

-- MongoDB相关end
-------------------


-------------------
-- Redis相关start

function do_common_redis(cmd, ...)
    local ok, success, r = pcall(skynet.call, ".common_redis_pool", "lua", "COMMAND_EXECUTE", cmd, ...)
    if not ok then
        return false, success
    end
    return success, r
end

function check_online_info_in_redis(uid, servername, errmsg)
    local redisKey = "online_uid:" .. uid
    local success, onlineInfo = do_common_redis("HGETALL", redisKey)
    if not success then
        return false
    end

    if table.empty(onlineInfo) then
        LOG_FATAL("utils:check_online_info_in_redis, cannot find online info in redis", {uid=uid, servername=servername, errmsg=errmsg})
        return false
    end

    if servername ~= onlineInfo.servername then
        LOG_FATAL("utils:check_online_info_in_redis, game node not match", {uid=uid, servername=servername, onlineServername=onlineInfo.servername, errmsg=errmsg})
        return false
    end

    return true
end

function make_subscribe_redis_client(host, port, dbindex, pwd)
    local redis = require "db.myredis"

    return redis.RedisClientSubscribe.new {
        host     = assert(host),
        port     = assert(math.tointeger(port)),
        dbindex  = assert(math.tointeger(dbindex)),
        password = assert(pwd)
    }
end

function free_subscribe_redis_client(redisClient)
    if redisClient then
        redisClient:disconnect()
    end
end

-- Redis相关end
-------------------


-------------------
-- Timer相关start

function add_timer(timeoutSecs, callback, ...)
    timeoutSecs = tonumber(timeoutSecs)
    if not timeoutSecs or not callback then
        return nil
    end

    local args = table.pack(...)
    local function wrapper_callback()
        if callback then
            callback(table.unpack(args))
        end
    end
    skynet.timeout(timeoutSecs * 100, wrapper_callback)

    return function() callback = nil end
end

function cancel_timer(handlerFunc)
    if handlerFunc then
        handlerFunc()
    end
end

-- Timer相关end
-------------------
