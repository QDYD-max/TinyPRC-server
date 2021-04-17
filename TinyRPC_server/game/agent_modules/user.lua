local skynet = require "skynet"
local mongo = require "db.mymongo"

local HANDLE

local DB_SAVE_KEY = "user"
local dbData = {
    uid         = nil,
    playerName  = nil
}

local user = {}

function user.bind(handle)
    HANDLE = handle
end

function user.serialize()
    return DB_SAVE_KEY, dbData
end

function user.deserialize(doc)
    if not table.empty(doc) then
        dbData = doc[DB_SAVE_KEY]
    end
end

function user.create(playerName)
    local uid = HANDLE.agent_call("get_uid")
    local success, doc, updated, upsert_id = skynet.call(".mongopool", "lua",
            "find_and_update", PDEFINE.MONGO_COLLECTION.GAME_AGENT, uid,
            { uid = uid },
            { ["$setOnInsert"] = { uid = uid } },
            { uid = 1 },
            {
                upsert          = true,
                new             = true,
                writeConcern    = mongo.WRITE_CONCERN_MAJORITY
            }
    )
    if not success or not doc or not doc.uid then
        return PDEFINE.RET.PLAYER_ERROR.CREATE_FAILED
    end
    if not upsert_id then
        return PDEFINE.RET.PLAYER_ERROR.PLAYER_EXISTS
    end

    dbData.uid = uid
    dbData.playerName = playerName
    HANDLE.agent_call("user_created")
    return PDEFINE.RET.SUCCESS, { uid = uid, playerName = playerName }
end

function user.get_login_info()
    local needCreate = dbData.uid and 0 or 1;
    return PDEFINE.RET.SUCCESS, needCreate
end

return user
