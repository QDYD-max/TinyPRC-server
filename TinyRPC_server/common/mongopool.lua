local skynet = require "skynet"
require "skynet.manager"
local mongo = require "db.mymongo"

local maxconn, addresses, authdb, db, user, pwd

local CMD = {}

local sessions = {}

local mongoClientList

local MongoClientListNode = class()
function MongoClientListNode:ctor(mongoClient)
    self.mongoClient = mongoClient
    self.prev = nil
    self.next = nil
    self.alive = true
    skynet.fork(self.ping, self)
end

function MongoClientListNode:ping(interval)
    if type(interval) ~= "number" or interval <= 0 then
        interval = 5
    end
    if interval < 1 then
        interval = 1
    end
    while self.mongoClient do
        self.alive = false
        skynet.fork(self.ping_, self)
        skynet.sleep(interval * 100)
        if not self.alive then
            LOG_ERROR("MongoClientListNode:ping, ping failed, maybe connection temporarily unavailable", {mongoClient=self.mongoClient})
        end

        if mongoClientList:in_ping_failed(self) and self.alive then
            mongoClientList:remove_ping_failed(self)
            mongoClientList:add(self, true)
        elseif not mongoClientList:in_ping_failed(self) and not self.alive then
            mongoClientList:remove(self)
            mongoClientList:add_ping_failed(self)
        end
    end
end

function MongoClientListNode:ping_()
    if self.mongoClient then
        local success, r = pcall(self.mongoClient.runCommand, self.mongoClient, "ping")
        if success and r.ok == 1 then
            self.alive = true
        else
            LOG_FATAL("mongopool:MongoClientListNode:ping_, ping failed, maybe mongos or shard have some problems", {mongoClient=self.mongoClient})
        end
    end
end

local MongoClientList = class()
function MongoClientList:ctor()
    self.head = nil
    self.cur = nil
    self.len = 0
    self.pingFailed = {}
end

function MongoClientList:add(mongoClientListNode, needRandom)
    if self.head then
        local beforeNode = self.head
        if needRandom then
            for _ = 1, math.random(self.len) do
                beforeNode = beforeNode.next
            end
        end
        mongoClientListNode.prev = beforeNode.prev
        beforeNode.prev.next = mongoClientListNode
        mongoClientListNode.next = beforeNode
        beforeNode.prev = mongoClientListNode
    else
        mongoClientListNode.prev = mongoClientListNode
        mongoClientListNode.next = mongoClientListNode
        self.head = mongoClientListNode
        self.cur = mongoClientListNode
    end
    self.len = self.len + 1
end

function MongoClientList:remove(mongoClientListNode)
    if mongoClientListNode.next == mongoClientListNode then
        self.head = nil
        self.cur = nil
    else
        if self.head == mongoClientListNode then
            self.head = mongoClientListNode.next
        end
        if self.cur == mongoClientListNode then
            self.cur = mongoClientListNode.next
        end
        mongoClientListNode.next.prev = mongoClientListNode.prev
        mongoClientListNode.prev.next = mongoClientListNode.next
    end
    mongoClientListNode.prev = nil
    mongoClientListNode.next = nil
    self.len = self.len - 1
end

function MongoClientList:peek()
    local cur = self.cur
    if self.cur then
        self.cur = self.cur.next
    end
    return cur and cur.mongoClient or nil
end

function MongoClientList:add_ping_failed(mongoClientListNode)
    self.pingFailed[mongoClientListNode] = 1
end

function MongoClientList:remove_ping_failed(mongoClientListNode)
    self.pingFailed[mongoClientListNode] = nil
end

function MongoClientList:in_ping_failed(mongoClientListNode)
    return self.pingFailed[mongoClientListNode]
end

function MongoClientList:destroy()
    while self.head do
        local mongoClientListNode = self.head
        self:remove(mongoClientListNode)
        mongoClientListNode.mongoClient:disconnect()
    end
    for mongoClientListNode, _ in ipairs(self.pingFailed) do
        mongoClientListNode.mongoClient:disconnect()
    end
    self.pingFailed = {}
end

local function runCommand(collection, sessionKey, func, ...)
    local mongoClient
    if sessionKey then
        if not sessions[collection] then
            sessions[collection] = {}
        end
        local session = sessions[collection][sessionKey]
        if session then
            mongoClient = session.client
            session.refCnt = session.refCnt + 1
        end
    end
    if not mongoClient then
        mongoClient = mongoClientList:peek()
        if not mongoClient then
            return false, string.format("mongopool:runCommand, no any available mongoClients, collection=%s, sessionKey=%s, func=%s", collection, sessionKey, func)
        end
        if sessionKey then
            sessions[collection][sessionKey] = {client = mongoClient, refCnt = 1}
        end
    end

    local mongoCollection = mongoClient:getDB(db):getCollection(collection)
    local retValues = table.pack(mongoCollection[func](mongoCollection, ...))

    if sessionKey then
        local session = sessions[collection][sessionKey]
        if session then
            session.refCnt = session.refCnt - 1
            if session.refCnt <= 0 then
                sessions[collection][sessionKey] = nil
            end
        end
    end

    return table.unpack(retValues, 1, retValues.n)
end


--region Basic
function CMD.insert_one(collection, sessionKey, doc, options)
    return runCommand(collection, sessionKey, "insert_one", doc, options)
end

function CMD.insert_bulk(collection, docs, options)
    return runCommand(collection, nil, "insert_bulk", docs, options)
end

function CMD.find_one(collection, sessionKey, query, filter, options)
    return runCommand(collection, sessionKey, "find_one", query, filter, options)
end

--[[
function CMD.find_one_linearize(collection, uniqueKey, query, filter, options)
    return runCommand(collection, uniqueKey, "find_one_linearize", query, filter, options)
end
--]]

function CMD.find_many(collection, query, filter, options)
    local mongoClient = mongoClientList:peek()
    if not mongoClient then
        return false, string.format("mongopool:find_many, no any available mongoClients, collection=%s", collection)
    end
    local mongoCollection = mongoClient:getDB(db):getCollection(collection)

    local allResults = {}
    options = table.empty(options) and {} or options
    local batchSize = options.batchSize
    if math.type(batchSize) ~= "integer" or batchSize <= 0 then
        batchSize = 50 -- avoid return too many results one time
    end
    options.batchSize = batchSize
    local success, results, cursor = mongoCollection["find_many"](mongoCollection, query, filter, options)
    if not success then
        return false, results
    end
    for _, result in ipairs(results) do
        table.insert(allResults, result)
    end
    while cursor ~= 0 do
        success, results, cursor = mongoCollection["get_more"](mongoCollection, cursor, {batchSize = batchSize})
        if not success then
            return false, results
        end
        for _, result in ipairs(results) do
            table.insert(allResults, result)
        end
    end
    return true, allResults
end

function CMD.update_one(collection, sessionKey, query, update, options)
    return runCommand(collection, sessionKey, "update_one", query, update, options)
end

function CMD.update_many(collection, query, update, options)
    return runCommand(collection, nil, "update_many", query, update, options)
end

function CMD.find_and_update(collection, sessionKey, query, update, filter, options)
    return runCommand(collection, sessionKey, "find_and_update", query, update, filter, options)
end

function CMD.find_and_remove(collection, sessionKey, query, filter, options)
    return runCommand(collection, sessionKey, "find_and_remove", query, filter, options)
end

function CMD.delete_one(collection, sessionKey, query, options)
    return runCommand(collection, sessionKey, "delete_one", query, options)
end

function CMD.delete_all(collection, query, options)
    return runCommand(collection, nil, "delete_all", query, options)
end

function CMD.aggregate(collection, pipeline, options)
    local mongoClient = mongoClientList:peek()
    if not mongoClient then
        return false, string.format("mongopool:aggregate, no any available mongoClients, collection=%s", collection)
    end
    local mongoCollection = mongoClient:getDB(db):getCollection(collection)

    local allResults = {}
    options = table.empty(options) and {} or options
    local batchSize = options.batchSize
    if math.type(batchSize) ~= "integer" or batchSize <= 0 then
        batchSize = 50 -- avoid return too many results one time
    end
    options.batchSize = batchSize
    local success, results, cursor = mongoCollection["aggregate"](mongoCollection, pipeline, options)
    if not success then
        return false, results
    end
    for _, result in ipairs(results) do
        table.insert(allResults, result)
    end
    while cursor ~= 0 do
        success, results, cursor = mongoCollection["get_more"](mongoCollection, cursor, {batchSize = batchSize})
        if not success then
            return false, results
        end
        for _, result in ipairs(results) do
            table.insert(allResults, result)
        end
    end
    return true, allResults
end
--endregion


function CMD.new_connection(addressidx)
    addressidx = addressidx or math.random(#addresses)
    addressidx = addressidx % #addresses + 1
    local mongoClient = mongo.MongoClient.new {
        host = addresses[addressidx][1],
        port = addresses[addressidx][2],
        authdb = authdb,
        username = user,
        password = pwd
    }

    local success, r = pcall(mongoClient.runCommand, mongoClient, "ping")
    if success and r.ok == 1 then
        LOG_INFO("mongopool:new_connection, new connection established", {mongoClient=mongoClient})
    else
        error("mongopool:new_connection error, connect to mongo failed")
    end

    local mongoClientListNode = MongoClientListNode.new(mongoClient)
    mongoClientList:add(mongoClientListNode)
end

function CMD.start(config)
    local addrstr
    if not config then
        maxconn = math.tointeger(skynet.getenv("mongo_maxconn")) or 10
        addrstr = assert(skynet.getenv("mongo_address"))
        authdb  = assert(skynet.getenv("mongo_authdb"))
        db      = assert(skynet.getenv("mongo_db"))
        user    = assert(skynet.getenv("mongo_user"))
        pwd     = assert(skynet.getenv("mongo_pwd"))
    else
        maxconn = math.tointeger(config.maxconn) or 10
        addrstr = assert(config.address)
        authdb  = assert(config.authdb)
        db      = assert(config.db)
        user    = assert(config.user)
        pwd     = assert(config.pwd)
    end
    if maxconn <= 0 then
        maxconn = 10
    end
    addresses = {}
    for host, port in string.gmatch(addrstr, "([^:,]+):([^,]+)") do
        table.insert(addresses, {host, math.tointeger(port)})
    end
    assert(#addresses > 0, "mongopool:start, no any mongo addresses")
    LOG_INFO("mongopool:CMD:start", {maxconn=maxconn, address=addresses, authdb=authdb, db=db, user=user})

    sessions = {}

    mongoClientList = MongoClientList.new()
    for idx = 1, maxconn do
        CMD.new_connection(idx)
    end

    if not config then
        skynet.register("." .. SERVICE_NAME)
    else
        if config.servicename then
            skynet.register("." .. config.servicename)
        end
    end
end

function CMD.stop()
    sessions = {}
    mongoClientList:destroy()
end

function CMD.exit()
    skynet.exit()
end

skynet.start(function()
    skynet.dispatch("lua", function(session, source, cmd, ...)
        local f = assert(CMD[cmd], cmd .. " not found")
        skynet.retpack(f(...))
    end)
end)