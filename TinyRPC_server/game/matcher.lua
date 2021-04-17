local skynet = require "skynet"
require "skynet.manager"

local STATE_WAITING_MATCH   = 1
local STATE_ASSIGN_ROOM     = 2
local STATE_NOTIFY_AGENT    = 3

local CMD = {}

local runningId = 0

local matchMembers
local waitingMatchCnt

function CMD.start_match(uid, agent)
    if matchMembers[uid] then
        return false, "already start match"
    end

    matchMembers[uid] = { uid = uid, agent = agent ,status = STATE_WAITING_MATCH }
    waitingMatchCnt = waitingMatchCnt + 1
    return true
end

function CMD.cancel_match(uid)
    if not matchMembers[uid] then
        return true
    end

    local matchInfo = matchMembers[uid]
    if matchInfo.status ~= STATE_WAITING_MATCH then
        return false, "already matched"
    end

    matchMembers[uid] = nil
    waitingMatchCnt = waitingMatchCnt - 1
    return true
end

local function assign_room(uidList)
    LOG_INFO("assign_room", {uidList = uidList})

    local room = skynet.newservice("room")
    for _, uid in ipairs(uidList) do
        local matchInfo = matchMembers[uid]
        matchInfo.status = STATE_NOTIFY_AGENT
        skynet.call(matchInfo.agent, "lua", "module_call", "combat", "match_success", room)
        --LOG_INFO("call_room", {uid = uid})
        matchMembers[uid] = nil
    end
end

local function match_loop(runId, intervalSec)
    intervalSec = intervalSec or 1
    while runId == runningId do
        while waitingMatchCnt >= 2 do
            local uidList = {}
            local cnt = 0
            for uid, matchInfo in pairs(matchMembers) do
                if matchInfo.status == STATE_WAITING_MATCH then
                    table.insert(uidList, uid)
                    matchInfo.status = STATE_ASSIGN_ROOM
                    waitingMatchCnt = waitingMatchCnt - 1
                    cnt = cnt + 1
                    if cnt >= 2 then
                        break
                    end
                end
            end

            skynet.fork(assign_room, uidList)
        end

        skynet.sleep(intervalSec * 100)
        --LOG_INFO("match_loop idle", {waitingMatchCnt=waitingMatchCnt})
    end
end

function CMD.start()
    matchMembers = {}
    waitingMatchCnt = 0

    runningId = runningId + 1
    skynet.fork(match_loop, runningId, 1)
end

skynet.start(function()
    skynet.dispatch("lua", function(session, address, cmd, ...)
        local f = assert(CMD[cmd], cmd .. " not found")
        skynet.retpack(f(...))
    end)
    skynet.register("." .. SERVICE_NAME)
end)
