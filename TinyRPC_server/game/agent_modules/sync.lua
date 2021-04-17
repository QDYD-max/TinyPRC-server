local skynet = require "skynet"

local HANDLE

local sync = {}

function sync.bind(handle)
    HANDLE = handle
end

function sync.get_location(loc)
    local room = skynet.call(skynet.self(), "lua", "module_call", "combat", "get_room")

    skynet.send(room, "lua", "location_got", HANDLE.agent_call("get_uid"),loc)
    
end

function sync.sync_location(loc)
    HANDLE.agent_call("notify_to_client", "NotifySyncLocation",loc)
end


function sync.get_sound(sd_id)
    local room = skynet.call(skynet.self(), "lua", "module_call", "combat", "get_room")

    skynet.send(room, "lua", "sound_got", HANDLE.agent_call("get_uid"),sd_id)
    
end

function sync.sync_sound(sd_id)
    HANDLE.agent_call("notify_to_client", "NotifySyncSound",sd_id)
end


function sync.get_effect(effect)
    local room = skynet.call(skynet.self(), "lua", "module_call", "combat", "get_room")
    
    skynet.send(room, "lua", "effect_got", HANDLE.agent_call("get_uid"),effect)
    
end

function sync.sync_effect(effect)
    HANDLE.agent_call("notify_to_client", "NotifySyncEffect",effect)
end

function sync.get_damage(damage)
    local room = skynet.call(skynet.self(), "lua", "module_call", "combat", "get_room")
    
    skynet.send(room, "lua", "damage_got", HANDLE.agent_call("get_uid"),damage)
    
end

function sync.sync_damage(damage)
    HANDLE.agent_call("notify_to_client", "NotifySyncDamage",damage)
end


function sync.get_wall_tags(wall_tags)
    local room = skynet.call(skynet.self(), "lua", "module_call", "combat", "get_room")
    
    skynet.send(room, "lua", "wall_got", HANDLE.agent_call("get_uid"),wall_tags)
    
end

function sync.sync_wall(wall_tags)
    HANDLE.agent_call("notify_to_client", "NotifySyncWall",wall_tags)
end

return sync