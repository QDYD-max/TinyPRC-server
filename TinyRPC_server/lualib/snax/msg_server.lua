local skynet = require "skynet"
require "skynet.manager"
local gateserver = require "snax.gateserver"
local socketdriver = require "skynet.socketdriver"
local netpack = require "skynet.netpack"
local crypt = require "skynet.crypt"

local userOnline    = {}  -- username -> u
local connection    = {}  -- fd -> u
local handshake     = {}  -- 需要握手的连接列表, fd -> address
local inHandshake   = {}  -- 正在握手的连接列表, fd -> address

local server = {}

function server.parse_username(username)
    -- base64(uid)@base64(server)#base64(subid)
    local uid, servername, token = string.match(username, "([^@]*)@([^#]*)#(.*)")
    return crypt.base64decode(uid), crypt.base64decode(token), crypt.base64decode(servername)
end

function server.gen_username(uid, token, servername)
    -- base64(uid)@base64(server)#base64(subid)
    return string.format("%s@%s#%s", crypt.base64encode(uid), crypt.base64encode(servername), crypt.base64encode(token))
end

function server.get_ip_by_username(username)
    local u = userOnline[username]
    if u and u.fd then
        return u.ip
    end
    return nil
end

function server.login(username, secret)
    assert(userOnline[username] == nil)
    userOnline[username] = {
        username    = username,
        secret      = secret,
        version     = 0,
        index       = 0,
        fd          = nil,
        ip          = nil,
        response    = {}    -- response cache
    }
end

function server.logout(username, uid)
    local u = userOnline[username]
    userOnline[username] = nil
    local lastfd = u and u.fd or nil
    if lastfd then
        connection[lastfd] = nil
        gateserver.closeclient(lastfd)
    end
end

function server.start(gamenodeGated)
    local expired_number = gamenodeGated.expired_number or 128

    local handler = {}

    local CMD = {
        login   = assert(gamenodeGated.login),
        logout  = assert(gamenodeGated.logout),
        collect = assert(gamenodeGated.collect),
        kick    = assert(gamenodeGated.kick)
    }
    assert(gamenodeGated.on_listen)
    assert(gamenodeGated.on_connect)
    assert(gamenodeGated.on_disconnect)
    assert(gamenodeGated.on_heartbeat)
    assert(gamenodeGated.on_request)

    -- 内部命令处理
    function handler.command(cmd, _, ...)
        local f = assert(CMD[cmd])
        return f(...)
    end

    -- 网关服务器创建监听的回调
    function handler.open(_, conf)
        local netInfo = assert(conf.netInfo)
        local servername = assert(conf.servername)
        return gamenodeGated.on_listen(netInfo, servername)
    end

    -- 网关服务器接收客户端连接的回调
    function handler.connect(fd, address)
        LOG_INFO("msg_server:server:start:handler:connect, login game node step 1, accept client connection and wait handshake", {fd=fd, address=address})
        handshake[fd] = address
        gateserver.openclient(fd)
    end

    -- 网关服务器与客户端连接断开的回调
    function handler.disconnect(fd)
        handshake[fd] = nil
        local u = connection[fd]
        if u then
            LOG_INFO("msg_server:server:start:handler:disconnect, client disconnected", {fd=fd, username=u.username})
            connection[fd] = nil
            xpcall(gamenodeGated.on_disconnect, __TRACEBACK__, u.username)
        end
    end

    -- socket发生错误时回调
    function handler.error(fd, msg)
        LOG_ERROR("msg_server:server:start:handler:error", {fd=fd, msg=msg})
        handler.disconnect(fd)
    end

    -- atomic , no yield
    local function do_auth(fd, address, message)
        LOG_INFO("msg_server:server:start:do_auth", {fd=fd, address=address, message=message})

        local username, index, clientReceivedSeq, hmac = string.match(message, "([^:]*):([^:]*):([^:]*):([^:]*)")
        local u = userOnline[username]
        if not u then
            LOG_ERROR("msg_server:server:start:do_auth, unauthorized", {username=username, index=index, clientReceivedSeq=clientReceivedSeq, hmac=hmac})
            return PDEFINE.RET.LOGIN_ERROR.UNAUTHORIZED
        end

        index = assert(math.tointeger(index))
        if index <= u.version then
            LOG_ERROR("msg_server:server:start:do_auth, index expired", {username=username, version=u.version, index=index, clientReceivedSeq=clientReceivedSeq, hmac=hmac})
            return PDEFINE.RET.LOGIN_ERROR.INDEX_EXPIRED
        end

        clientReceivedSeq = assert(math.tointeger(clientReceivedSeq))

        local text = string.format("%s:%s:%s", username, index, clientReceivedSeq)
        hmac = crypt.base64decode(hmac)
        if hmac ~= crypt.hmac64(crypt.hashkey(text), u.secret) then
            LOG_ERROR("msg_server:server:start:do_auth, hmac error", {username=username, index=index, clientReceivedSeq=clientReceivedSeq, hmac=hmac})
            return PDEFINE.RET.LOGIN_ERROR.HMAC_ERROR
        end

        -- 处理notify重传
        if clientReceivedSeq >= 0 then
            local ok, ret = pcall(gamenodeGated.can_reconnected, u.username, clientReceivedSeq)
            if not ok or not ret then
                LOG_ERROR("msg_server:server:start:do_auth, clientReceivedSeq expired", {username=username, index=index, clientReceivedSeq=clientReceivedSeq, hmac=hmac})
                -- 返回重大错误，触发服客户端回到登录界面，一般情况下并不会出现，只可能客户端断网或者切后台很长时间
                return PDEFINE.RET.LOGIN_ERROR.NOTIFY_EXPIRED
            end
        end

        -- 可能来自重连建立新fd，需要检查持有的老fd回收
        if u.fd and connection[u.fd] and connection[u.fd].username == username then
            LOG_INFO("msg_server:server:start:do_auth, reconnect and release old fd", {oldfd=u.fd, fd=fd, username=username})
            connection[u.fd] = nil
        end

        u.version = index
        u.fd = fd
        u.ip = address
        connection[fd] = u

        LOG_INFO("msg_server:server:start:do_auth, login game node step 2, handshake success", {fd=fd, address=address, username=username, index=index, clientReceivedSeq=clientReceivedSeq})

        -- on_connect保存fd到agent,不能有消息推送
        local ok, retCode = xpcall(gamenodeGated.on_connect, __TRACEBACK__, fd, username)
        if not ok then
            retCode = PDEFINE.RET.LOGIN_ERROR.CALL_ERROR
        end
        return retCode, username, clientReceivedSeq
    end

    local function auth(fd, address, msg, sz)
        -- 上次握手未结束
        if inHandshake[fd] then
            return
        end
        inHandshake[fd] = true

        local message = netpack.tostring(msg, sz)
        LOG_INFO("msg_server:server:start:auth", {fd=fd, address=address, message=message})

        local ok, retCode, username, clientReceivedSeq = pcall(do_auth, fd, address, message)
        if not ok then
            LOG_ERROR("msg_server:server:start:auth, do_auth call failed", {errmsg=retCode, fd=fd, address=address, message=message})
            retCode = PDEFINE.RET.LOGIN_ERROR.CALL_ERROR
        elseif retCode ~= PDEFINE.RET.SUCCESS then
            LOG_ERROR("msg_server:server:start:auth, do_auth failed", {fd=fd, address=address, message=message, username=username})
        end
        inHandshake[fd] = nil

        handshake[fd] = nil -- 握手验证消息返回,客户端可能马上就有协议消息包,提前结束握手状态避免重复握手
        socketdriver.send(fd, netpack.pack(string.pack(">I2", retCode)))
        if retCode ~= PDEFINE.RET.SUCCESS then
            gateserver.closeclient(fd)
            return
        end

        LOG_INFO("msg_server:server:start:auth, login game node step 3, agent connected", {fd=fd, address=address, username=username, clientReceivedSeq=clientReceivedSeq})

        -- flush clientReceivedSeq
        pcall(gamenodeGated.flush_notify, username, clientReceivedSeq)
    end

    -- u.response is a struct { return_fd , response, version, index }
    local function retire_response(u)
        if u.index >= expired_number * 2 then
            local max = 0
            local response = u.response
            for k, p in pairs(response) do
                if p[1] == nil then
                    -- request complete, check expired
                    if p[4] < expired_number then
                        response[k] = nil
                    else
                        p[4] = p[4] - expired_number
                        if p[4] > max then
                            max = p[4]
                        end
                    end
                end
            end
            u.index = max + 1
        end
    end

    local function do_request(fd, message)
        local u = assert(connection[fd], "invalid fd")
        local size = #message
        if size == 1 then -- 心跳
            xpcall(gamenodeGated.on_heartbeat, __TRACEBACK__, u.username)
            local msg = string.pack(">I1", 0)
            socketdriver.send(fd, string.pack(">s2", msg))
            return
        end

        -- message数据 [消息数据 + 4字节session + 12字节hmac]
        -- 解析数据尾部的16字节session+base64(hmac)
        local tailMsg = message:sub(-16)
        local session = tailMsg:sub(1,4)
        local hmac = crypt.base64decode(tailMsg:sub(5))
        local p = u.response[session]
        if p then
            -- session can be reuse in the same connection
            if p[3] == u.version and p[2] == nil then
                LOG_ERROR("msg_server:server:start:do_request, session conflict", {fd=fd, session=crypt.hexencode(session)})
                error("msg_server:server:start:do_request, session conflict")
            end
        end
        session = string.unpack(">i4", session)
        if p == nil then
            if tonumber(session) > 0 then
                p = { fd }
                u.response[session] = p
            end
            local hmacMsg = message:sub(1,-13)
            if hmac ~= crypt.hmac64(crypt.hashkey(hmacMsg), u.secret) then
                LOG_ERROR("msg_server:server:start:do_request, hmac error", {fd=fd, hmac=hmac, hmacMsg=hmacMsg})
                error("msg_server:server:start:do_request, hmac error")
            end

            message = message:sub(1,-17)
            local ok, ret = xpcall(gamenodeGated.on_request, __TRACEBACK__, u.username, message)
            if not ok then
                LOG_ERROR("msg_server:server:start:do_request, request error", {fd=fd, username=u.username, errmsg=ret})
                error("msg_server:server:start:do_request, request error")
            end
            if tonumber(session) == 0 then
                return
            end
            -- NOTICE: YIELD here, socket may close.
            ret = ret or ""
            ret = ret .. string.pack(">i4", session)
            ret = ret .. crypt.base64encode(crypt.hmac64(crypt.hashkey(ret), u.secret)) -- 带上校验码
            p[2], p[3], p[4] = string.pack(">s2", ret), u.version, u.index
        else
            -- update version/index, change return fd.
            -- resend response.
            LOG_INFO("msg_server:server:start:do_request, reuse session", {fd=fd, session=crypt.hexencode(session), version=u.version, index=u.index})
            p[1], p[3], p[4] = fd, u.version, u.index
            if p[2] == nil then
                LOG_ERROR("msg_server:server:start:do_request, already request but response is not ready")
                return
            end
        end

        u.index = u.index + 1
        -- the return fd is p[1] (fd may change by multi request) check connect
        fd = p[1]
        if connection[fd] then
            socketdriver.send(fd, p[2])
        else
            LOG_ERROR("msg_server:server:start:do_request, request send back but connection is nil", {fd=fd})
        end
        p[1] = nil
        retire_response(u)
    end

    local function request(fd, msg, sz)
        local message = netpack.tostring(msg, sz)
        local ok, errmsg = pcall(do_request, fd, message)
        -- not atomic, may yield
        if not ok then
            LOG_ERROR("msg_server:server:start:request, invalid package", {errmsg=errmsg})
            if connection[fd] then
                gateserver.closeclient(fd)
            end
        end
    end

    -- socket消息到来时回调，新连接的第一条消息是握手消息
    function handler.message(fd, msg, sz)
        local address = handshake[fd]
        if address then
            auth(fd, address, msg, sz)
        else
            request(fd, msg, sz)
        end
    end

    skynet.register_protocol {
        name    = "client",
        id      = skynet.PTYPE_CLIENT
    }

    return gateserver.start(handler)
end

return server
