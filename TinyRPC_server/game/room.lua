local skynet = require "skynet"
require "skynet.manager"

local STATE_PLAYER_INITIAL   = 1
local STATE_READY_FINISH     = 2
local STATE_NOTIFY_AGENT     = 3

local CMD = {}

local members = {}

local readyNum = 0
local reNum = 0
local room_bout = 0
local player_oprerater_flag = false
local player_finish_flag = false
local launchCache
local curId = 0
local game_over_flag = false
local assign_flag = true


function CMD.enter_room(uid, agent,agent_name)
    if members[uid] then
        return false, "already in room"
    end

    members[uid] = { uid = uid, agent = agent ,name = agent_name,status = STATE_PLAYER_INITIAL}

    local otherMembers = {}

    for u, memberInfo in pairs(members) do
        if u ~= uid then
            skynet.send(memberInfo.agent, "lua", "module_call", "combat", "other_player_enter_room", uid)
            -- LOG_INFO("member enter room", {uid = uid})
            table.insert(otherMembers, u)
        end
    end
    return true, otherMembers
end

--分边
function CMD.assign_side()

    local i = 0
    local random_seed = skynet.time()
    LOG_INFO("time",{random_seed = random_seed})
    for u, memberInfo in pairs(members) do
        LOG_INFO("assign side to player", {uid = u})
        local side_seed = { side = i,seed = random_seed }
        skynet.call(memberInfo.agent, "lua", "module_call", "assign", "send_side", side_seed)
        i = i+1
        for user, otherMembers in pairs(members) do
            if user ~= u then
                skynet.send(memberInfo.agent, "lua", "module_call", "assign", "send_name", otherMembers.name)
            end
        end
    end

    local Match_ready_time = skynet.now()
    if i == 2 then
        skynet.fork(CMD.WaitPlayerPick(Match_ready_time))
    end
    LOG_INFO("ready to start time")
    return true
end

function CMD.play_ready(uid,ballArr)
    --LOG_INFO("Length mmmmmmmmmm", { length = #members})
    for u, memberInfo in pairs(members) do
        if u ~= uid then
            LOG_INFO("Opponent is ready", { uid = uid , other = u})
            skynet.send(memberInfo.agent, "lua", "module_call", "play", "send_to_oppon",ballArr)
            LOG_INFO("I am ready", { uid = uid , readyNum = readyNum })
            memberInfo.status = STATE_READY_FINISH
            readyNum = readyNum + 1
        end
    end
    return true
end

--等待玩家选人√
function CMD.WaitPlayerPick(Match_ready_time)
    LOG_INFO("Has Start Time")
    while readyNum ~= 2 and skynet.now() - Match_ready_time < 3000 do
        LOG_INFO("WaitPlayerPick Fork")
        skynet.sleep(10)
        --等待玩家选人结束
    end
    for u, memberInfo in pairs(members) do
        skynet.send(memberInfo.agent, "lua", "module_call", "play", "all_are_ready")
    end

    CMD.round_start()
end

--回合控制
function CMD.round_start()
    player_oprerater_flag = false
    player_finish_flag = false
    launchCache = 0
    curId = 0
    CMD.AddBout()  --回合数+1

    local start_time = skynet.now()
    skynet.fork(CMD.WaitPlayerOperate(start_time))  --等待玩家操作或者回合结束
    return true
end
--增加回合数
function CMD.AddBout()
    room_bout = room_bout + 1
end
--等待玩家操作
function CMD.WaitPlayerOperate(start_time)
    while player_oprerater_flag == false and skynet.now() - start_time < 2000 do
        local remain_time = 2000 - start_time
        for u, memberInfo in pairs(members) do
            --LOG_INFO("player send local", { uid = uid , other = u})
            skynet.send(memberInfo.agent, "lua", "module_call", "play", "notify_time",remain_time)
        end
        --等待回合结束
        skynet.sleep(10)
    end
    for u, memberInfo in pairs(members) do
        --LOG_INFO("player send local", { uid = uid , other = u})
        if u ~= curId then
            --LOG_INFO("player send launchAttr", { uid = uid , other = u})
            skynet.send(memberInfo.agent, "lua", "module_call", "play", "synch_launch", launchCache)
        end
    end
    while player_oprerater_flag == true and player_finish_flag == false do
        skynet.sleep(10)
    end
    for u, memberInfo in pairs(members) do
        skynet.send(memberInfo.agent, "lua", "module_call", "play", "notify_next_round")
    end
    CMD.round_start()
    skynet.exit()
end

function CMD.balls_all_ready()
    LOG_INFO("balls all are ready")
    player_finish_flag = true
end
--end

function CMD.play_launch(uid,launchAttr)
    player_oprerater_flag = true
    curId = uid
    launchCache = launchAttr
    
    return true
end

function CMD.location_got(uid,loc)
    for u, memberInfo in pairs(members) do
        if u ~= uid then
            --LOG_INFO("player send local", { uid = uid , other = u})
            skynet.send(memberInfo.agent, "lua", "module_call", "location", "synch_location", loc)
        end
    end
end

function CMD.this_room_game_over(uid,isWin)
    game_over_flag = true
    for u, memberInfo in pairs(members) do
        if u ~= uid then
            --LOG_INFO
            skynet.send(memberInfo.agent, "lua", "module_call", "play", "notify_game_over",isWin)
        end
    end
end
function CMD.this_room_re_game(uid,isRe)
    --LOG_INFO("player",{ isRe = isRe.Re})
    if isRe.Re == true then
        reNum = reNum + 1
    else
        for u, memberInfo in pairs(members) do
            if u ~= uid then
                skynet.call(memberInfo.agent, "lua", "module_call", "play", "notify_room_over")
                LOG_INFO("opponent exit")
                CMD.this_room_over()
            end
        end
    end
    if reNum == 2 then
        for u, memberInfo in pairs(members) do
            --LOG_INFO("player send local", { uid = uid , other = u})
            skynet.call(memberInfo.agent, "lua", "module_call", "play", "notify_game_re")
        end
        LOG_INFO("use room initial")
        CMD.room_initial()
        LOG_INFO("re assign")
        CMD.assign_side()
    end
end

function CMD.room_initial() 
    readyNum = 0
    reNum = 0
    room_bout = 0
    player_oprerater_flag = false
    player_finish_flag = false
    curId = 0
    game_over_flag = false
    assign_flag = true
    for u, memberInfo in pairs(members) do
        LOG_INFO("room initial")
        memberInfo.status = STATE_PLAYER_INITIAL 
    end
end

function CMD.this_room_over()
    for u, memberInfo in pairs(members) do
        skynet.call(memberInfo.agent, "lua", "module_call", "play", "notify_room_over")
        members[u] = nil
        LOG_INFO("player clean")
    end
    LOG_INFO("this room over rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr")
    skynet.exit()
    LOG_INFO("this room over nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn")
end

skynet.start(function()
    skynet.dispatch("lua", function(session, address, cmd, ...)
        local f = assert(CMD[cmd], cmd .. " not found")
        skynet.retpack(f(...))
    end)
end)