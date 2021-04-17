local skynet = require "skynet"
local socket = require "skynet.socket"
local crypt = require "skynet.crypt"
local random = require "random"

local socketErr = {} -- 标记socket错误
local function assert_socket(v, fd)
    if v then return v end
    LOG_ERROR("login_slave:assert_socket, socket error marked", {fd=fd})
    error(socketErr)
end

local function write(fd, data)
    -- 每次write 2字节数据长度(大端编码) + 数据
    local msg = string.pack(">s2", data)
    assert_socket(socket.write(fd, msg), fd)
end

local function read(fd)
    -- 每次read 2字节数据长度(大端编码) + 数据
    local len = assert_socket(socket.read(fd, 2), fd)
    local sz = len:byte(1) * 256 + len:byte(2)
    return assert_socket(socket.read(fd, sz), fd)
end

local function gen_new_login_token()
    local seed = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'}
    local buffer = {}
    for _ = 1, 16 do
        table.insert(buffer, seed[random.Get(1, #seed)])
    end
    return table.concat(buffer)
end

local function gen_new_game_uid(SDKId, loginPid)
    local gameUid = skynet.call(".account_mgr", "lua", "get_next_id")
    if not gameUid or gameUid <= 0 or gameUid >= 100000000 then
        LOG_ERROR("login_slave:gen_new_game_uid, gen game uid failed")
        return nil
    end

    gameUid = (gameUid % 8 + 1) * 100000000 + gameUid
    local accountDoc = {
        uid     = gameUid,
        sdkid   = SDKId,
        pid     = loginPid
    }
    local success = skynet.call(".account_mgr", "lua", "new_account", accountDoc)
    if not success then
        LOG_ERROR("login_slave:gen_new_game_uid, create new account failed")
        return nil
    end

    return gameUid
end

local function auth_channel(SDKId, userInfo)
    LOG_INFO("login_slave:auth_channel, login step 3", {SDKId=SDKId, userInfo=userInfo})

    local userInfoList = string.split(userInfo, "#")
    local user = userInfoList[1] -- 账号

    local gameUid
    local account = skynet.call(".account_mgr", "lua", "get_account", SDKId, user)
    if table.empty(account) then
        gameUid = gen_new_game_uid(SDKId, user)
        if not gameUid then
            LOG_ERROR("login_slave:auth_channel, gen_new_game_uid failed", {SDKId=SDKId, user=user})
            return PDEFINE.RET.LOGIN_ERROR.REGISTER_ERROR, 0
        end
    else
        gameUid = account.uid
    end

    return PDEFINE.RET.SUCCESS, gameUid
end

local function auth_token(gameUid, loginToken)
    LOG_INFO("login_slave:auth_token, login step 3", {gameUid=gameUid, loginToken=loginToken})

    local token = skynet.call(".account_mgr", "lua", "get_login_token", gameUid)
    if not token or token ~= loginToken then
        return PDEFINE.RET.LOGIN_ERROR.TOKEN_ERROR, 0
    end

    return PDEFINE.RET.SUCCESS
end

local function auth_login(token, address)
    LOG_INFO("login_slave:auth_login, login step 2", {token=token, address=address})

    local tokenInfo = string.split(token, "#")
    local params = string.split(tokenInfo[1], ":")

    local isToken = params[1]
    if isToken == "0" then
        local SDKId = crypt.base64decode(params[2])
        local userInfo = crypt.base64decode(params[3])
        local retCode, gameUid = auth_channel(SDKId, userInfo)
        if retCode ~= PDEFINE.RET.SUCCESS then
            LOG_ERROR("login_slave:auth_login, auth_channel error", {retCode=retCode, error=gameUid})
            return retCode, { error = gameUid }
        end
        return PDEFINE.RET.SUCCESS, {
            gameUid         = gameUid,
            refreshToken    = true
        }
    elseif isToken == "1" then
        local loginToken = crypt.base64decode(params[2])
        local gameUid = math.tointeger(crypt.base64decode(params[3]))
        local retCode, errcode = auth_token(gameUid, loginToken)
        if retCode ~= PDEFINE.RET.SUCCESS then
            return retCode, { error = errcode }
        end
        return PDEFINE.RET.SUCCESS, {
            gameUid         = gameUid,
            refreshToken    = false
        }
    else
        LOG_ERROR("login_slave:auth_login, give a invalid isToken param", {token=token, address=address, isToken=isToken})
        return PDEFINE.RET.LOGIN_ERROR.PARAMS_ERROR, { error = 0 }
    end
end

local function auth_connection(fd, address)
    LOG_INFO("login_slave:auth_connection, login step 1", {fd=fd, address=address})

    -- 1. S2C : base64(8bytes random challenge)随机串，用于后序的握手验证
    local challenge = crypt.randomkey()
    write(fd, crypt.base64encode(challenge))

    -- 2. C2S : base64(8bytes handshake client key)由客户端发送过来随机串，用于交换 secret 的 key
    local handshake = read(fd)
    local clientKey = crypt.base64decode(handshake)
    if #clientKey ~= 8 then
        write(fd, string.pack(">I2", PDEFINE.RET.LOGIN_ERROR.SOCKET_ERROR))
        LOG_ERROR("login_slave:auth_connection, client send a invalid key", {fd=fd, address=address, clientKey=clientKey})
        return PDEFINE.RET.LOGIN_ERROR.SOCKET_ERROR
    end

    -- 3. S: Gen a 8bytes handshake server key生成一个用户交换 secret 的 key
    local serverKey = crypt.randomkey()

    -- 4. S2C : base64(DH-Exchange(server key))利用 DH 密钥交换算法，发送交换过的 server key
    write(fd, crypt.base64encode(crypt.dhexchange(serverKey)))

    -- 5. S/C secret := DH-Secret(client key/server key)服务器和客户端都可以计算出同一个 8 字节的 secret
    local secret = crypt.dhsecret(clientKey, serverKey)

    -- 6. C2S : base64(HMAC(challenge, secret))回应服务器第一步握手的挑战码，确认握手正常
    local response = read(fd)
    local hmac = crypt.hmac64(challenge, secret)
    if hmac ~= crypt.base64decode(response) then
        write(fd, string.pack(">I2", PDEFINE.RET.LOGIN_ERROR.HMAC_ERROR))
        LOG_ERROR("login_slave:auth_connection, challenge failed", {fd=fd, address=address})
        return PDEFINE.RET.LOGIN_ERROR.HMAC_ERROR
    end

    -- 7. C2S : DES(secret, base64(token))使用 DES 算法，以 secret 做 key 加密传输 token
    local etoken = read(fd)
    local token = crypt.desdecode(secret, crypt.base64decode(etoken))

    -- 8. S : call auth_handler(token) -> server, uid
    local success, retCode, retValue = xpcall(auth_login, __TRACEBACK__, token, address)
    if not success then
        write(fd, string.pack(">I2", PDEFINE.RET.LOGIN_ERROR.CALL_ERROR))
        LOG_ERROR("login_slave:auth_connection, auth_login failed", {fd=fd, address=address, errmsg=retCode})
        return PDEFINE.RET.LOGIN_ERROR.CALL_ERROR
    end

    if retCode ~= PDEFINE.RET.SUCCESS then
        write(fd, string.pack(">I2", retCode) .. crypt.base64encode(retValue.error))
        LOG_ERROR("login_slave:auth_connection, auth_login return error", {fd=fd, address=address, error=retValue.error})
        return retCode
    end

    return PDEFINE.RET.SUCCESS, retValue, secret
end

local function auth(fd, address)
    local function handle_auth(success, retCode, ...)
        if not success then
            if retCode == socketErr then
                return PDEFINE.RET.LOGIN_ERROR.SOCKET_ERROR
            end
            return PDEFINE.RET.LOGIN_ERROR.CALL_ERROR
        end
        return retCode, ...
    end
    return handle_auth(pcall(auth_connection, fd, address))
end

local function login_gamenode(gameUid, refreshToken, secret)
    LOG_INFO("login_slave:login_gamenode, login step 4", {gameUid=gameUid, refreshToken=refreshToken, secret=secret})
    
    local redisKey = "online_uid:" .. gameUid
    local success, onlineInfo = do_common_redis("HGETALL", redisKey)
    if not success then
        LOG_ERROR("login_slave:login_gamenode, get last onlineInfo error")
        return PDEFINE.RET.LOGIN_ERROR.CALL_ERROR
    end

    if not table.empty(onlineInfo) then
        local kicked = skynet.call(".gamegated", "lua", "kick", gameUid, onlineInfo.token)
        if not kicked then
            LOG_ERROR("login_slave:login_gamenode, kick online player failed", {gameUid=gameUid, servername=onlineInfo.servername, address=onlineInfo.address, token=onlineInfo.token})
            return PDEFINE.RET.LOGIN_ERROR.ALREADY_LOGIN
        end
        LOG_INFO("login_slave:login_gamenode, kick online player success", {gameUid=gameUid})
    end

    local token = skynet.call(".gamegated", "lua", "login", gameUid, secret)

    local newOnlineInfo = {
        servername  = "game",
        address     = ".gamegated",
        token       = token
    }
    local success = do_common_redis("HSET", redisKey, newOnlineInfo)
    if not success then
        return PDEFINE.RET.LOGIN_ERROR.CALL_ERROR
    end 
    
    local loginToken
    if refreshToken then
        loginToken = skynet.call(".account_mgr", "lua", "set_login_token", gameUid, gen_new_login_token())
    else
        loginToken = skynet.call(".account_mgr", "lua", "get_login_token", gameUid)
    end
    if not loginToken then
        LOG_ERROR("login_slave:login_gamenode, get loginToken error")
        return PDEFINE.RET.LOGIN_ERROR.CALL_ERROR
    end
    local ip = skynet.getenv("gameip")
    local port = skynet.getenv("gameport")
    local gamenodeInfo = {
        servername  = "gamenode",
        netInfo     = ip .. ":" .. port
    }
    LOG_INFO("gamenodeInfo", gamenodeInfo)
    return PDEFINE.RET.SUCCESS, gamenodeInfo, token, loginToken
end

local function login_gate(fd, authInfo, secret)
    local gameUid = authInfo.gameUid
    local refreshToken = authInfo.refreshToken

    local redisKey = "login_uid:" .. gameUid
    local success, set = do_common_redis("SET", redisKey, 1, "EX", 10*PDEFINE.SECONDS.MINUTE, "NX")
    if not success or not set then
        write(fd, string.pack(">I2", PDEFINE.RET.LOGIN_ERROR.ALREADY_LOGIN))
        LOG_ERROR("login_slave:login_gate, login locked by redis", {gameUid=gameUid})
        return PDEFINE.RET.LOGIN_ERROR.ALREADY_LOGIN
    end
    local retCode, gamenodeInfo, gameToken, loginToken = login_gamenode(gameUid, refreshToken, secret)
    do_common_redis("DEL", redisKey)

    gameToken = gameToken or ""
    if retCode ~= PDEFINE.RET.SUCCESS then
        write(fd, string.pack(">I2", retCode) .. crypt.base64encode(gameToken))
        return retCode
    end

    LOG_INFO("login_slave:login_gate", {gameUid=gameUid, gameToken=gameToken, gamenodeInfo=gamenodeInfo, loginToken=loginToken})
    write(fd, table.concat({
        string.pack(">I2", PDEFINE.RET.SUCCESS),
        crypt.base64encode(gameUid .. ":" .. gameToken),
        "@",
        crypt.base64encode(gamenodeInfo.servername),
        "#",
        crypt.base64encode(gamenodeInfo.netInfo),
        "#",
        crypt.base64encode(loginToken)
    }))
    return PDEFINE.RET.SUCCESS
end

local function login(fd, authInfo, secret)
    local function handle_login(success, retCode, ...)
        if not success then
            if retCode == socketErr then
                return PDEFINE.RET.LOGIN_ERROR.SOCKET_ERROR
            end
            return PDEFINE.RET.LOGIN_ERROR.CALL_ERROR
        end
        return retCode, ...
    end
    return handle_login(xpcall(login_gate,__TRACEBACK__,  fd, authInfo, secret))
end

local CMD = {}

function CMD.accept(fd, address)
    LOG_INFO("login_slave:CMD:accept", {fd=fd, address=address})

    socket.start(fd)
    socket.limit(fd, 8192) -- set socket buffer limit (8K). If the attacker send large package, close the socket
    
    local retCode, authInfo, secret = auth(fd, address)
    if retCode ~= PDEFINE.RET.SUCCESS then
        return retCode
    end
    
    retCode = login(fd, authInfo, secret)
    if retCode ~= PDEFINE.RET.SUCCESS then
        return retCode
    end
    
    socket.abandon(fd)
    return PDEFINE.RET.SUCCESS
end

skynet.start(function()
    skynet.dispatch("lua", function(session, address, cmd, ...)
        local f = assert(CMD[cmd], cmd .. " not found")
        skynet.retpack(f(...))
    end)
end)