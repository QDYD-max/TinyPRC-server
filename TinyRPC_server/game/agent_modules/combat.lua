local skynet = require "skynet"

local HANDLE

local combat = {}

local agent_room

local agent_name

function combat.bind(handle)
    HANDLE = handle
end

function combat.start_match(name)
    agent_name = name
    local success, errmsg = skynet.call(".matcher", "lua", "start_match", HANDLE.agent_call("get_uid"), skynet.self())
    --LOG_INFO("combat.start_match", {success=success, errmsg=errmsg})
    if success ~= true then
        LOG_INFO("combat.start_match erro",  {errmsg=errmsg})
    else
        HANDLE.agent_call("notify_to_client", "NotifyStartMatch")
    end
end

function combat.cancel_match()
    local success, errmsg = skynet.call(".matcher", "lua", "cancel_match", HANDLE.agent_call("get_uid"))
    --LOG_INFO("combat.cancel_match", {success=success, errmsg=errmsg})
    if success ~= true then
        LOG_INFO("combat.cancel_match erro",  {errmsg=errmsg})
    else
        HANDLE.agent_call("notify_to_client", "NotifyCancelMatch")
    end
end

function combat.match_success(room)
    --LOG_INFO("combat.match_success", {room = room})
    agent_room = room

    local success, otherMembers = skynet.call(room, "lua", "enter_room", HANDLE.agent_call("get_uid"), skynet.self(),agent_name)
    --LOG_INFO("combat.match_success", {success=success, otherMembers=otherMembers})
    if success ~= true then
        LOG_INFO("combat.match_success error",otherMembers)
    else
        HANDLE.agent_call("notify_to_client", "NotifyMatchSuccess",otherMembers)
        skynet.send(room, "lua", "assign_side")
    end
end

function combat.other_player_enter_room(uid)
    --LOG_INFO("combat.other_player_enter_room", {otherUid = uid})
    -- 处理别的玩家进入房间
    local uid_send = {}
    table.insert(uid_send,uid)
    HANDLE.agent_call("notify_to_client", "NotifyOtherPlayerEnterRoom", uid_send)
end

function combat.get_room()
    return agent_room
end

return combat