local skynet = require "skynet"
require "skynet.manager"
local codecache = require "skynet.codecache"
codecache.mode("OFF")
local sharedata = require "skynet.sharedata"

local CMD = {}

function CMD.start()
    require "AllCfgInfo"
    sharedata.new("NCONFIG", NCONFIG)
end

function CMD.reload()
    LOG_INFO("configmgr:CMD:reload, start")
    local startTime = skynet.now()

    require "AllCfgInfo"
    sharedata.update("NCONFIG", NCONFIG)

    local finishTime = skynet.now()
    LOG_INFO("configmgr:CMD:reload, finish", {usedTime=(finishTime-startTime)/100})
end

skynet.start(function()
    skynet.dispatch("lua", function(session, address, cmd, ...)
        local f = assert(CMD[cmd], cmd .. " not found")
        skynet.retpack(f(...))
    end)
    skynet.register("." .. SERVICE_NAME)
end)