local skynet = require "skynet"
require "skynet.manager"

skynet.start(function()
    LOG_INFO(">--------- Server Launch Begin --------->")

    -- 启动配置表管理服务
    skynet.uniqueservice("configmgr")
    skynet.call(".configmgr", "lua", "start")

    -- 启动MongoDB连接池
    local mongopool = skynet.newservice("mongopool")
    skynet.call(mongopool, "lua", "start")

    -- 启动Redis连接池
    local redispool = skynet.newservice("redispool")
    skynet.call(redispool, "lua", "start", {
        maxconn     = skynet.getenv("common_redis_maxconn"),
        host        = skynet.getenv("common_redis_host"),
        port        = skynet.getenv("common_redis_port"),
        dbindex     = skynet.getenv("common_redis_index"),
        pwd         = skynet.getenv("common_redis_pwd"),
        servicename = "common_redis_pool"
    })

    -- 启动账号管理服务
    skynet.uniqueservice("account_mgr")
    skynet.call(".account_mgr", "lua", "start")
    -- 启动登陆监听和处理服务
    skynet.uniqueservice("login_master")
    skynet.call(".login_master", "lua", "start", math.tointeger(skynet.getenv("login_slave_num")))
    skynet.call(".login_master", "lua", "listen", "0.0.0.0", math.tointeger(skynet.getenv("port")))

    local ip = skynet.getenv("gameip")
    local port = assert(math.tointeger(skynet.getenv("gameport")))
    local netInfo = ip .. ":" .. port
    local servername = "gamenode"

    local gated = skynet.uniqueservice("gated")
    skynet.call(gated, "lua", "open" , {
        ip          = ip,
        port        = port,
        netInfo     = netInfo,
        servername  = servername
    })
    skynet.name(".gamegated", gated)

    --匹配器的启动
    local matcher = skynet.uniqueservice("matcher")
    skynet.call(matcher, "lua", "start")


    LOG_INFO("<--------- Server Launch Finish ---------<")
    skynet.exit()
end)