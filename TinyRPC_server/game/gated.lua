local skynet = require "skynet"
local msgserver = require "snax.msg_server"

local AGENT_MAX_LIMIT = math.tointeger(skynet.getenv("maxclient")) or 1024

local NET_INFO
local SERVER_NAME

local uid2agentInfo = {}
local username2agentInfo = {}
local token = 0

local agentNum
local agentPoolSize
local agentPool

local function new_agent()
    local agent = skynet.newservice("agent")
    table.insert(agentPool, agent)
    agentNum = agentNum + 1
    agentPoolSize = agentPoolSize + 1
    LOG_DEBUG("gated:new_agent, create new game agent and push to pool", {agentNum=agentNum, agentPoolSize=agentPoolSize})
end

local function init_agent_pool(maxPoolSize)
    LOG_INFO("gated:init_agent_pool", {maxPoolSize=maxPoolSize})
    agentNum = 0
    agentPoolSize = 0
    agentPool = {}
    for _ = 1, maxPoolSize do
        new_agent()
    end
end

local function adjust_agent_pool(maxPoolSize)
    while true do
        if agentNum < AGENT_MAX_LIMIT and agentPoolSize < maxPoolSize then
            new_agent()
            if agentPoolSize < maxPoolSize // 10 then
                skynet.sleep(10)
            else
                skynet.sleep(100)
            end
        else
            skynet.sleep(1000)
        end
    end
end

local function get_agent()
    local agent = table.remove(agentPool)
    if agent then
        agentPoolSize = agentPoolSize - 1
        LOG_DEBUG("gated:get_agent, get agent from pool", {agentNum=agentNum, agentPoolSize=agentPoolSize})
        return agent
    end

    if agentNum >= AGENT_MAX_LIMIT then
        LOG_FATAL("gated:get_agent, agent reach max limit", {agentNum=agentNum, agentMaxLimit=AGENT_MAX_LIMIT})
        return nil
    end

    agent = skynet.newservice("agent")
    agentNum = agentNum + 1
    LOG_DEBUG("gated:get_agent, create new agent", {agentNum=agentNum, agentPoolSize=agentPoolSize})
    return agent
end

local server = {}

-- 从login节点那边调用过来，此时客户端还没有跟node连接
function server.login(uid, secret)
    if uid2agentInfo[uid] then
        LOG_ERROR("gated:server:login, already login", {uid=uid, secret=secret})
        error("gated:server:login, already login")
    end

    token = token + 1
    local tk = token -- prevent later changed
    local username = msgserver.gen_username(uid, tk, SERVER_NAME)

    local agent = get_agent()
    if not agent then
        LOG_ERROR("gated:server:login, agent reach max limit", {uid=uid, token=tk, servername=SERVER_NAME})
        error("gated:server:login, agent reach max limit")
    end

    local agentInfo = {
        uid         = uid,
        token       = tk,
        username    = username,
        agent       = agent,
    }
    skynet.call(agent, "lua", "on_login", SERVER_NAME, uid, tk, secret)

    if uid2agentInfo[uid] then
        LOG_FATAL("gated:server:login, agent info changed", {agentInfoOld=agentInfo, agentInfo=uid2agentInfo[uid]})
        error("gated:server:login, agent info changed")
    end

    LOG_INFO("gated.login", agentInfo)

    uid2agentInfo[uid] = agentInfo
    username2agentInfo[username] = agentInfo

    msgserver.login(username, secret)

    return tk
end

-- 从login节点那边调用过来, 尝试把之前那个玩家踢掉
function server.kick(uid, tk)
    local agentInfo = uid2agentInfo[uid]
    if not agentInfo then
        return true
    end

    assert(agentInfo.username == msgserver.gen_username(uid, tk, SERVER_NAME))

    local ok, kicked = pcall(skynet.call, agentInfo.agent, "lua", "on_kick")
    if not ok or not kicked then
        LOG_FATAL("gated:server:kick, agent kick failed", {uid=uid, token=tk, servername=SERVER_NAME})
        return false
    end

    return true
end

-- 从agent那边调过来，主动中断与客户端的网络连接
function server.logout(uid, tk)
    local agentInfo = uid2agentInfo[uid]
    if not agentInfo then
        LOG_ERROR("gated:server:logout, no agent info", {uid=uid, token=tk, servername=SERVER_NAME})
        return
    end

    assert(agentInfo.username == msgserver.gen_username(uid, tk, SERVER_NAME))
    msgserver.logout(agentInfo.username, uid)
end

-- 从agent那边调过来，标记agent进入回收状态
function server.collect(uid, tk)
    local agentInfo = uid2agentInfo[uid]
    if not agentInfo then
        LOG_ERROR("gated:server:collect, no agent info", {uid=uid, token=tk, servername=SERVER_NAME})
        return
    end

    assert(agentInfo.username == msgserver.gen_username(uid, tk, SERVER_NAME))
    uid2agentInfo[uid] = nil
    username2agentInfo[uid] = nil

    if check_online_info_in_redis(uid, SERVER_NAME, "gated:server:collect") then
        local redisKey = "online_uid:" .. uid
        do_common_redis("DEL", redisKey)
    end

    skynet.timeout(PDEFINE.SECONDS.MINUTE * 100, function()
        pcall(skynet.send, agentInfo.agent, "lua", "on_exit")
        agentNum = agentNum - 1
        LOG_INFO("gated:server:collect, collect agent finish", {uid=uid, token=tk, servername=SERVER_NAME, agentNum=agentNum})
    end)
end

-- 服务器监听启动
function server.on_listen(netInfo, servername)
    LOG_INFO("gated:server:on_listen", {netInfo=netInfo, servername=servername, agentMaxLimit=AGENT_MAX_LIMIT})

    NET_INFO    = netInfo       -- "ip:port"
    SERVER_NAME = servername    -- [env]servername

    init_agent_pool(AGENT_MAX_LIMIT // 4)
    skynet.fork(adjust_agent_pool, AGENT_MAX_LIMIT // 4)
end

-- 收到客户端握手包
function server.on_connect(fd, username)
    local agentInfo = username2agentInfo[username]
    if not agentInfo then
        return PDEFINE.RET.LOGIN_ERROR.UNAUTHORIZED
    end

    assert(username == agentInfo.username and agentInfo.agent)
    return skynet.call(agentInfo.agent, "lua", "on_connect", fd, username)
end

-- 与客户端断开连接
function server.on_disconnect(username)
    local agentInfo = username2agentInfo[username]
    if agentInfo then
        assert(username == agentInfo.username and agentInfo.agent)
        skynet.call(agentInfo.agent, "lua", "on_disconnect")
    end
end

function server.can_reconnected(username, clientReceivedSeq)
    local agentInfo = username2agentInfo[username]
    if agentInfo then
        assert(username == agentInfo.username and agentInfo.agent)
        return skynet.call(agentInfo.agent, "lua", "can_reconnected", clientReceivedSeq)
    end
end

function server.flush_notify(username, clientReceivedSeq)
    local agentInfo = username2agentInfo[username]
    if agentInfo then
        assert(username == agentInfo.username and agentInfo.agent)
        skynet.call(agentInfo.agent, "lua", "flush_notify", clientReceivedSeq)
    end
end

-- 收到客户端心跳包
function server.on_heartbeat(username)
    local agentInfo = username2agentInfo[username]
    assert(agentInfo and username == agentInfo.username and agentInfo.agent)
    skynet.rawsend(agentInfo.agent, "client", "p")
end

-- 收到客户端请求包
function server.on_request(username, msg)
    local agentInfo = username2agentInfo[username]
    assert(agentInfo and username == agentInfo.username and agentInfo.agent)
    return skynet.tostring(skynet.rawcall(agentInfo.agent, "client", msg))
end

msgserver.start(server)