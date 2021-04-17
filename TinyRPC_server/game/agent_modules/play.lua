local skynet = require "skynet"
local json =  require "json"

local HANDLE

local play = {}

function play.bind(handle)
    HANDLE = handle
end

--准备阶段
--玩家准备
function play.ball_ready(ballArr)
    local room = skynet.call(skynet.self(), "lua", "module_call", "combat", "get_room")

    skynet.send(room, "lua", "play_ready", HANDLE.agent_call("get_uid"),ballArr)
end

--对手已经准备完毕
function play.send_to_oppon(ballArr)
    --LOG_INFO( "player send arr")
    local arr_send = {}
    table.insert(arr_send,ballArr)
    HANDLE.agent_call("notify_to_client", "NotifyBallReady",ballArr)
end

--玩家全部准备完毕
function play.all_are_ready()
    HANDLE.agent_call("notify_to_client", "NotifyFightStart")
end
--准备阶段end


--发射阶段
--玩家发射
function play.id_ball_launch(launchAttr)
    local room = skynet.call(skynet.self(), "lua", "module_call", "combat", "get_room")

    skynet.send(room, "lua", "play_launch", HANDLE.agent_call("get_uid"),launchAttr)
end

--同步给对手发射信息
function play.synch_launch(launchAttr)
    HANDLE.agent_call("notify_to_client", "NotifyBallLaunch",launchAttr)
end
--发射阶段end

-- 
-- 
-- 
-- 
function play.notify_time(remain_time)
    local send_time = { time = remain_time}
    --HANDLE.agent_call("notify_to_client", "NotifySychTime",send_time)
end
--等待球停止信号
function play.all_balls_stop()
    local room = skynet.call(skynet.self(), "lua", "module_call", "combat", "get_room")

    skynet.send(room, "lua", "balls_all_ready")
end
--
-- 
-- 
-- 



--回合结束阶段
--下一回合
-- function play.round_over()
--     local room = skynet.call(skynet.self(), "lua", "module_call", "combat", "get_room")

--     skynet.send(room, "lua", "next_round")
-- end

--通知玩家下一回合
function play.notify_next_round(bout_count)
    local bout_send = { bout_count=bout_count }
    HANDLE.agent_call("notify_to_client", "NotifyNextRound",bout_send)
end
--回合结束阶段end


--游戏结束阶段
--通知房间
function play.game_over(isWin)
    local room = skynet.call(skynet.self(), "lua", "module_call", "combat", "get_room")

    skynet.send(room, "lua", "this_room_game_over", HANDLE.agent_call("get_uid"),isWin)
end

--通知玩家游戏结束
function play.notify_game_over(isWin)
    HANDLE.agent_call("notify_to_client", "NotifyGameOver",isWin)
end
--游戏结束阶段end



--是否需要重来
--通知房间
function play.re_game(isRe)
    local room = skynet.call(skynet.self(), "lua", "module_call", "combat", "get_room")

    local jsIsRe = json.parse(isRe)

    skynet.send(room, "lua", "this_room_re_game", HANDLE.agent_call("get_uid"), jsIsRe)
end
--通知玩家重新开始
function play.notify_game_re()
    HANDLE.agent_call("notify_to_client", "NotifyReGame")
end



--通知玩家游戏结束
function play.notify_game_over(isWin)
    HANDLE.agent_call("notify_to_client", "NotifyGameOver",isWin)
end
--游戏结束阶段end

--房间清理接口
--通知房间
function play.room_over()
    local room = skynet.call(skynet.self(), "lua", "module_call", "combat", "get_room")

    skynet.send(room, "lua", "this_room_over", HANDLE.agent_call("get_uid"))
end

--房间清理通知
function play.notify_room_over()
    HANDLE.agent_call("notify_to_client", "NotifyRoomClean")
end

--房间清理end



return play