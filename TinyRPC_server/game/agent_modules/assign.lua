local skynet = require "skynet"

local HANDLE

local assign = {}

function assign.bind(handle)
    HANDLE = handle
end

function assign.send_side(side_seed)
    HANDLE.agent_call("notify_to_client", "NotifyAssign", side_seed)
end

function assign.send_name(name)
    HANDLE.agent_call("notify_to_client", "NotifyMatchSuccess", name)
end

return assign