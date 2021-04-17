local skynet = require "skynet"

local HANDLE

local rpctester = {}

function rpctester.bind(handle)
    HANDLE = handle
end

function rpctester.ping(a, str, tbl)
    LOG_INFO("rpc_test", a, str, tbl)
    return PDEFINE.RET.SUCCESS, { uid = HANDLE.agent_call("get_uid"), pong = {a, str, tbl} }
end

function rpctester.errortest()
    LOG_INFO("errortest")
    return PDEFINE.RET.ERROR.ERRORTEST
end

return rpctester
