local skynet = require "skynet"
local sharedata = require "skynet.sharedata"
local mongo = require "db.mymongo"
local msgpack = require "msgpack"
local socket = require "skynet.socket"
local netpack = require "skynet.netpack"
local crypt = require "skynet.crypt"

local MONGO_SAVE_TIME = math.tointeger(skynet.getenv("mongo_save_time")) or 5 * 60

local GATED
local SERVER_NAME
local UID
local TOKEN
local SECRET
local FD
local base64 = require "skynet.crypt"

-- 注意这些状态是有序的，即可能进行范围判断
local AGENT_STATE = {
    INIT        = 1, -- 新创建
    LOGIN       = 2, -- login节点验证登录通过，但还未连接agent
    CONNECTED   = 3, -- node节点验证连接通过，已经连接agent，但数据未加载完成
    ONLINE      = 4, -- 已经加载完毕数据
    LOGOUT      = 5, -- 在登出过程中
    DESTROY     = 6  -- 登出逻辑已经执行结束，等待虚拟机被销毁
}
local STATE = AGENT_STATE.INIT

-- 模块列表
local moduleList = {
    ["user"]  = require "agent_modules.user",
    ["notifytester"]  = require "agent_modules.notifytester",
    ["rpctester"]  = require "agent_modules.rpctester",
    ["combat"]  = require "agent_modules.combat",
    ["assign"]  = require "agent_modules.assign",
    ["sync"]  = require "agent_modules.sync",
    ["play"]  = require "agent_modules.play",
}

-- 推送消息缓存
local rpcCacheQueue = require("rpcCacheQueue").new(100)

-- 断线超时Timer
local disconnectTimer

local autoSaveToken = 1
local function do_save(uid)
    LOG_INFO("agent:do_save", {uid=uid})
    local doc = {}
    for _, module in pairs(moduleList) do
        if module["serialize"] then
            local dbSaveKey, dbData = module.serialize()
            if dbSaveKey ~= nil then
                doc[dbSaveKey] = dbData
            end
        end
    end
    local success, update_n, modified_n, _ = skynet.call(".mongopool", "lua",
            "update_one", PDEFINE.MONGO_COLLECTION.GAME_AGENT, uid,
            { uid = uid },
            { ["$setOnInsert"] = { uid = uid }, ["$set"] = doc },
            {
                upsert          = true,
                writeConcern    = mongo.WRITE_CONCERN_MAJORITY
            }
    )
    if not success then
        LOG_FATAL("agent:do_save, mongo operation failed", {uid=uid, doc=doc, errmsg=update_n})
    else
        if update_n ~= 1 then
            LOG_FATAL("agent:do_save, save data failed", {uid=uid, doc=doc, update_n=update_n, modified_n=modified_n})
        else
            LOG_INFO("agent:do_save, save data success", {uid=uid, update_n=update_n, modified_n=modified_n})
        end
    end
end

local function auto_save(uid, token)
    while STATE == AGENT_STATE.ONLINE and uid == UID and token == autoSaveToken do
        do_save(uid)
        skynet.sleep(MONGO_SAVE_TIME * 100)
    end
end

local function load_data(doc)
    LOG_INFO("agent:load_data", {uid=UID})
    for _, module in pairs(moduleList) do
        if module["deserialize"] then
            module.deserialize(doc)
        end
    end
end

local function do_logout()
    if STATE < AGENT_STATE.LOGIN or STATE > AGENT_STATE.ONLINE then
        return
    end

    -- 从这里开始，此agent不能再接受任何客户端的连接，也不能被重用

    local prevState = STATE
    STATE = AGENT_STATE.LOGOUT

    LOG_INFO("agent:do_logout, logout game node step 1, agent start logout", {prevState=prevState, state=STATE, servername=SERVER_NAME, uid=UID})

    -- 取消断线超时倒计时
    if disconnectTimer then
        cancel_timer(disconnectTimer)
        disconnectTimer = nil
    end

    LOG_INFO("agent:do_logout, logout game node step 2, all calling are finished", {prevState=prevState, state=STATE, servername=SERVER_NAME, uid=UID})

    -- 断开与客户端的连接
    if GATED then
        skynet.call(GATED, "lua", "logout", UID, TOKEN)
    end

    LOG_INFO("agent:do_logout, logout game node step 3, close client connection", {prevState=prevState, state=STATE, servername=SERVER_NAME, uid=UID})

    -- 登出前存盘
    if prevState == AGENT_STATE.ONLINE then
        autoSaveToken = autoSaveToken + 1
        do_save(UID)
    end

    -- 标记进入待销毁状态
    if GATED then
        skynet.call(GATED, "lua", "collect", UID, TOKEN)
    end

    STATE = AGENT_STATE.DESTROY

    LOG_INFO("agent:do_logout, logout game node step 4, agent logout success", {prevState=prevState, state=STATE, servername=SERVER_NAME, uid=UID})

    collectgarbage("collect")
end

local LOGIN_CALL = {}

function LOGIN_CALL.on_login(gated, servername, uid, token, secret)
    LOG_INFO("agent:LOGIN_CALL:on_login, called", {state=STATE, gated=gated, servername=servername, uid=uid, token=token, secret=secret})

    if STATE ~= AGENT_STATE.INIT then
        LOG_ERROR("agent:LOGIN_CALL:on_login, login cluster calling, but this agent is a online agent", {state=STATE, gated=gated, servername=servername, uid=uid, token=token, secret=secret})
        error("agent:LOGIN_CALL:on_login, login cluster calling, but this agent is a online agent")
    end

    GATED       = gated
    SERVER_NAME = servername
    UID         = uid
    TOKEN       = token
    SECRET      = secret

    STATE       = AGENT_STATE.LOGIN

    LOG_INFO("agent:LOGIN_CALL:on_login, finished", {state=STATE, gated=gated, servername=servername, uid=uid, token=token, secret=secret})
end

function LOGIN_CALL.on_kick(gated)
    LOG_INFO("agent:LOGIN_CALL:on_kick, called", {state=STATE, servername=SERVER_NAME, uid=UID, gated=gated})

    if STATE == AGENT_STATE.INIT then
        return false
    end

    local logoutWaitCnt = 10
    while STATE == AGENT_STATE.LOGOUT do
        if logoutWaitCnt <= 0 then
            return false
        end
        skynet.sleep(100)
        logoutWaitCnt = logoutWaitCnt - 1
    end

    if STATE == AGENT_STATE.DESTROY then
        return true
    end

    do_logout()

    LOG_INFO("agent:LOGIN_CALL:on_kick, finished", {state=STATE, servername=SERVER_NAME, uid=UID, gated=gated})

    return true
end

local GATE_CALL = {}

function GATE_CALL.on_connect(gated, fd, username)
    LOG_INFO("agent:GATE_CALL:on_connect, called", {state=STATE, servername=SERVER_NAME, uid=UID, gated=gated, fd=fd, username=username})

    if not (STATE == AGENT_STATE.LOGIN or STATE == AGENT_STATE.ONLINE) then
        return PDEFINE.RET.LOGIN_ERROR.UNAUTHORIZED
    end

    FD = fd

    -- 取消断线超时倒计时
    if disconnectTimer then
        cancel_timer(disconnectTimer)
        disconnectTimer = nil
    end

    if STATE == AGENT_STATE.LOGIN then
        STATE = AGENT_STATE.CONNECTED
        local success, doc = skynet.call(".mongopool", "lua",
                "find_one", PDEFINE.MONGO_COLLECTION.GAME_AGENT, UID,
                { uid = UID },
                nil,
                {
                    readPreference      = mongo.READ_PREFERENCE_PRIMARY, -- 强制从主库获取
                    allowPartialResults = false
                }
        )
        if not success then
            do_logout() -- 数据库加载失败，此agent强制废弃
            return PDEFINE.RET.LOGIN_ERROR.CALL_ERROR
        end

        if not table.empty(doc) then
            load_data(doc)
            STATE = AGENT_STATE.ONLINE

            autoSaveToken = autoSaveToken + 1
            skynet.fork(auto_save, UID, autoSaveToken)
        end
    end

    LOG_INFO("agent:GATE_CALL:on_connect, finished", {state=STATE, servername=SERVER_NAME, uid=UID, gated=gated, fd=fd, username=username})

    return PDEFINE.RET.SUCCESS
end

function GATE_CALL.on_disconnect(gated)
    LOG_INFO("agent:GATE_CALL:on_disconnect, called", {state=STATE, servername=SERVER_NAME, uid=UID, gated=gated})

    FD = nil

    if not disconnectTimer then
        disconnectTimer = add_timer(2*PDEFINE.SECONDS.MINUTE, do_logout)
    end

    LOG_INFO("agent:GATE_CALL:on_disconnect, finished", {state=STATE, servername=SERVER_NAME, uid=UID, gated=gated})
end

function GATE_CALL.can_reconnected(gated, clientReceivedSeq)
    LOG_INFO("agent:GATE_CALL:can_reconnected, called", {state=STATE, servername=SERVER_NAME, uid=UID, gated=gated, clientReceivedSeq=clientReceivedSeq})

    if STATE < AGENT_STATE.LOGIN or STATE > AGENT_STATE.ONLINE then
        return false
    end
    return not rpcCacheQueue:check_expired(clientReceivedSeq)
end

function GATE_CALL.flush_notify(gated, clientReceivedSeq)
    LOG_INFO("agent:GATE_CALL:flush_notify, called", {state=STATE, servername=SERVER_NAME, uid=UID, gated=gated, clientReceivedSeq=clientReceivedSeq, serverSeq=rpcCacheQueue:get_sequence()})

    if STATE == AGENT_STATE.ONLINE then
        if clientReceivedSeq < 0 then
            -- 客户端重登
            rpcCacheQueue:clear()
        else
            rpcCacheQueue:flush(clientReceivedSeq, function (msg)
                if msg then
                    if FD then
                        socket.write(FD, netpack.pack(msg))
                    end
                end
            end)
        end
    end
end

function GATE_CALL.on_exit()
    LOG_INFO("agent:NET_CALL:on_exit")
    skynet.exit()
end

local SERVER_CALL = {}

function SERVER_CALL.get_uid()
    return UID
end

function SERVER_CALL.user_created()
    STATE = AGENT_STATE.ONLINE

    autoSaveToken = autoSaveToken + 1
    skynet.fork(auto_save, UID, autoSaveToken)
end

-- 为了降低functionname字符串占用的协议流量，可以将rpcFunc转为int，客户端服务端共同维护int到string的映射，后期可以做成自动化。
function SERVER_CALL.notify_to_client(rpcFunc, ...)
    if FD ~= nil then
        local retObj = {}
        retObj.opCode = "NOTIFY_INFO"
        retObj.uid = UID

        retObj.notifyInfo = {}
        retObj.notifyInfo.sequence = rpcCacheQueue:get_sequence()
        retObj.notifyInfo.rpcFunc = rpcFunc
        retObj.notifyInfo.rpcParams = crypt.base64encode(msgpack.encode(...))

        local retMsg = message_pb_encode(retObj)
        if #retMsg > 65535 then -- 包内容超出约定2字节长度
            LOG_ERROR("agent:notify_to_client, ret msg too large", {msgLength=#retMsg, opCode=retObj.opCode})
            retObj = {
                opCode      = retObj.opCode,
                uid         = retObj.uid,
                response    = { errorCode = PDEFINE.RET.ERROR.PACKAGE_TOO_LARGE }
            }
            retMsg = message_pb_encode(retObj)
        end

        -- 广播给客户端的数据，后4字节session, 必须为0
        retMsg = retMsg .. string.pack(">I4", 0)
        -- 带上校验码
        retMsg = retMsg .. crypt.base64encode(crypt.hmac64(crypt.hashkey(retMsg), SECRET))
        socket.write(FD, netpack.pack(retMsg))

        rpcCacheQueue:push(retMsg)
    end
end


function SERVER_CALL.module_call(module, func, ...)
    return moduleList[module][func](...)
end

local HANDLE = {}

function HANDLE.module_call(module, func, ...)
    return moduleList[module][func](...)
end

function HANDLE.agent_call(func, ...)
    return SERVER_CALL[func](...)
end

local function rpc_msg_dispatch(recvObj, msg)
    -- 为了节省流量，这里可以转为int，然后维护int对string的映射。
    -- todo 后面做成自动化
    local func = recvObj.request.rpcFunc
    local moduleName, funcName = string.match(func, "([^.]*).(.*)")
    if not moduleList[moduleName] or not moduleList[moduleName][funcName] then
        return {
            opCode      = "RPC_CALL",
            uid         = UID,
            timeNow     = os.time(),
            response    = {
                errorCode   = PDEFINE.RET.ERROR.CALL_ERROR,
                rpcFunc     = func,
                rpcRsp      = msgpack.encode()
            }
        }
    end

    local function handle_module_call(success, retCode, ...)
        if not success then
            retCode = PDEFINE.RET.ERROR.CALL_ERROR
        end
        return {
            opCode      = "RPC_CALL",
            uid         = UID,
            timeNow     = os.time(),
            response    = {
                errorCode   = retCode,
                rpcFunc     = func,
                rpcRsp      = base64.base64encode(msgpack.encode(...))
            }
        }   
    end
    local username = msgpack.decode(recvObj.request.rpcParams)
    return handle_module_call(xpcall(moduleList[moduleName][funcName], __TRACEBACK__, username))
end

local function client_msg_dispatch(recvObj, msg)
    local errorCode
    if not recvObj then
        errorCode = PDEFINE.RET.ERROR.DECODE_FAIL
    elseif recvObj.uid ~= UID then
        errorCode = PDEFINE.RET.ERROR.CALL_ERROR
    elseif recvObj.opCode == "NONE" then
        errorCode = PDEFINE.RET.SUCCESS
    end
    if errorCode then
        return {
            opCode      = "NONE",
            uid         = UID,
            response    = { errorCode = errorCode }
        }
    end

    if recvObj.opCode == "RPC_CALL" then
        return rpc_msg_dispatch(recvObj, msg)
    end

    return {
        opCode      = "NONE",
        uid         = UID,
        response    = { errorCode = PDEFINE.RET.SUCCESS }
    }
end

skynet.start(function()
    skynet.register_protocol {
        name    = "client",
        id      = skynet.PTYPE_CLIENT,
        unpack  = skynet.tostring
    }

    skynet.dispatch("lua", function(session, address, cmd, ...)
        if LOGIN_CALL[cmd] then
            local f = LOGIN_CALL[cmd]
            skynet.retpack(f(address, ...))
        elseif GATE_CALL[cmd] then
            local f = GATE_CALL[cmd]
            skynet.retpack(f(address, ...))
        elseif SERVER_CALL[cmd] then
            if STATE >= AGENT_STATE.LOGIN and STATE <= AGENT_STATE.LOGOUT then
                local f = SERVER_CALL[cmd]
                skynet.retpack(f(...))
            else
                error("agent:start:dispatch_lua, agent unable call")
            end
        end
    end)

    skynet.dispatch("client", function(session, address, msg)
        if STATE < AGENT_STATE.CONNECTED or STATE > AGENT_STATE.ONLINE then
            error("agent:start:dispatch_client, agent unable call")
        end

        local retObj
        local success, recvObj = xpcall(message_pb_decode, __TRACEBACK__, msg)
        if success then
            if(type(recvObj) == 'table') then
                recvObj.request.rpcParams = base64.base64decode(recvObj.request.rpcParams)
            end
            success, retObj = xpcall(client_msg_dispatch, __TRACEBACK__, recvObj, msg)
        end
        if not success then
            retObj = {
                opCode      = "NONE",
                uid         = UID,
                response    = { errorCode = PDEFINE.RET.ERROR.CALL_ERROR }
            }
        end

        local retMsg = message_pb_encode(retObj)
        if #retMsg > 65535 then -- 包内容超出约定2字节长度
            LOG_ERROR("agent:start:dispatch_client, ret msg too large", {msgLength=#retMsg, opCode=retObj.opCode})
            retObj = {
                opCode      = retObj.opCode,
                uid         = retObj.uid,
                response    = { errorCode = PDEFINE.RET.ERROR.PACKAGE_TOO_LARGE }
            }
            retMsg = message_pb_encode(retObj)
        end
        skynet.ret(retMsg)
    end)

    NCONFIG = sharedata.query("NCONFIG")

    for _, module in pairs(moduleList) do
        module.bind(HANDLE)
        if module["_init_"] then
            module._init_()
        end
    end
end)