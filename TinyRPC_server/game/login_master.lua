local skynet = require "skynet"
require "skynet.manager"
local socket = require "skynet.socket"

local loginSlaveIndex
local loginSlaveServices
local function pick_login_slave_service()
    local service = loginSlaveServices[loginSlaveIndex]
    loginSlaveIndex = loginSlaveIndex + 1
    if loginSlaveIndex > #loginSlaveServices then
        loginSlaveIndex = 1
    end
    return service
end

local function new_login_slave_services(slaveNum)
    loginSlaveIndex = 1
    loginSlaveServices = {}
    slaveNum = slaveNum or 64
    for _ = 1, slaveNum do
        local loginSlaveService = skynet.newservice("login_slave")
        table.insert(loginSlaveServices, loginSlaveService)
    end
end

local CMD = {}

function CMD.start(slaveNum)
    new_login_slave_services(slaveNum)
    runningFlag = true
    LOG_INFO("login_master:CMD:start")
end

function CMD.listen(host, port)
    host = host or "0.0.0.0"
    assert(port)

    socket.start(socket.listen(host, port), function(fd, address)
        if runningFlag then
            local loginSlaveService = pick_login_slave_service()
            local success, retCode = xpcall(skynet.call, __TRACEBACK__, loginSlaveService, "lua", "accept", fd, address)
            if not success then
                retCode = PDEFINE.RET.LOGIN_ERROR.CALL_ERROR
            end
            if retCode ~= PDEFINE.RET.SUCCESS then
                LOG_ERROR("login_master:CMD:listen, login_slave accept failed", {retCode=retCode})
            else
                LOG_INFO("login_master:CMD:listen, login step 5", {fd=fd, address=address})
            end
            socket.close(fd)
        end
    end)

    LOG_INFO("login_master:CMD:listen", {host=host, port=port})
end

function CMD.shutdown()
    runningFlag = false
    LOG_INFO("login_master:CMD:shutdown")
end

skynet.start(function()
    skynet.dispatch("lua", function(session, address, cmd, ...)
        local f = assert(CMD[cmd], cmd .. " not found")
        skynet.retpack(f(...))
    end)
    skynet.register("." .. SERVICE_NAME)
end)