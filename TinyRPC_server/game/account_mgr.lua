local skynet = require "skynet"
require "skynet.manager"
local mongo = require "db.mymongo"

local function gen_union_key(SDKId, loginPid)
    return SDKId .. ":" .. loginPid
end

local CMD = {}

function CMD.get_next_id()
    local success, nextId = do_common_redis("INCR", "account_maxid")
    return success and nextId or nil
end

function CMD.new_account(accountDoc)
    if table.empty(accountDoc) or not accountDoc.uid then
        return false
    end

    local unionKey = gen_union_key(accountDoc.sdkid, accountDoc.pid)
    local success, insert_n = skynet.call(".mongopool", "lua",
            "insert_one", PDEFINE.MONGO_COLLECTION.ACCOUNT, unionKey,
            accountDoc,
            { writeConcern = mongo.WRITE_CONCERN_MAJORITY }
    )
    if not success or insert_n ~= 1 then
        LOG_FATAL("account_mgr:CMD:new_account, new account create failed", {errmsg=insert_n, accountDoc=accountDoc})
        return false
    end

    LOG_INFO("account_mgr:CMD:new_account, new account create success", {accountDoc=accountDoc})
    return true
end

function CMD.get_account(SDKId, loginPid)
    local unionKey = gen_union_key(SDKId, loginPid)
    local success, account = skynet.call(".mongopool", "lua",
            "find_one", PDEFINE.MONGO_COLLECTION.ACCOUNT, unionKey,
            { sdkid = SDKId, pid = loginPid },
            nil,
            {
                readPreference      = mongo.READ_PREFERENCE_PRIMARY, -- 强制从主库获取
                allowPartialResults = false
            }
    )
    assert(success)
    if not account then
        return nil
    end

    return account
end

function CMD.set_login_token(gameUid, loginToken)
    local redisKey = "account_login_token:" .. gameUid
    local success, set = do_common_redis("SET", redisKey, loginToken, "EX", PDEFINE.SECONDS.MONTH)
    if success and set then
        return loginToken
    end
    return nil
end

function CMD.get_login_token(gameUid)
    local redisKey = "account_login_token:" .. gameUid
    local success, token = do_common_redis("GET", redisKey)
    return success and token or nil
end

function CMD.start()
    -- add init codes here
end

skynet.start(function()
    skynet.dispatch("lua", function(session, address, cmd, ...)
        local f = assert(CMD[cmd], cmd .. " not found")
        skynet.retpack(f(...))
    end)
    skynet.register("." .. SERVICE_NAME)
end)