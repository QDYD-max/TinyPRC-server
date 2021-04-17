local skynet = require "skynet"
local json =  require "json"
local HANDLE

local notifytester = {}

function notifytester.bind(handle)
    HANDLE = handle
end

function notifytester.rpc_start_notify(times)
    print("start notification")
    local tb = json.parse(times)
    local str = json.stringify(tb)
    HANDLE.agent_call("notify_to_client", "NotifyTest", tb,"test")
    --return
    --for i = 1, times do
        --LOG_INFO("notify_to_client Lua_NetworkManager.NotifyTest")
        --local a = {}
        --for i = 0, 3 do
           --table.insert(a,"x")
        --end
        --HANDLE.agent_call("notify_to_client", "NotifyTest", a, "test")
    --end
end

return notifytester
