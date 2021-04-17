-- Support for MongoDB 4.0

local socketchannel = require "skynet.socketchannel"
local bson = require "bson"
local driver = require "skynet.mongo.driver"
local crypt = require "skynet.crypt"
local md5 = require "md5"

local table = table
local math = math
local string = string
local assert = assert
local error = error
local type = type
local setmetatable = setmetatable
local getmetatable = getmetatable
local pairs = pairs
local ipairs = ipairs
local bson_encode_order = bson.encode_order
local bson_decode = bson.decode


local USE_PRETTY_SHOW = false
local OPERATION_TOKEN = 1


local mongo = {}


local DEFAULT = setmetatable({}, {
    __metatable = "default_metatable",
    __newindex = function() error("Attempt to set value to DEFAULT", 2) end
})


local WRITE_CONCERN_PRIMARY = {concern = 1, journal = true} -- confirm write on primary
local WRITE_CONCERN_MAJORITY = {concern = "majority", journal = true} -- confirm write on majority
local WRITE_CONCERN_MAPPING = {
    WRITE_CONCERN_PRIMARY   = WRITE_CONCERN_PRIMARY,
    WRITE_CONCERN_MAJORITY  = WRITE_CONCERN_MAJORITY
}
mongo.WRITE_CONCERN_PRIMARY = "WRITE_CONCERN_PRIMARY"
mongo.WRITE_CONCERN_MAJORITY = "WRITE_CONCERN_MAJORITY"


local READ_CONCERN_LOCAL = {concern = "local"} -- read current result
local READ_CONCERN_MAJORITY = {concern = "majority"} -- read confirmed result
local READ_CONCERN_LINEARIZABLE = {concern = "linearizable"} -- read after write confirmed
local READ_CONCERN_MAPPING = {
    READ_CONCERN_LOCAL          = READ_CONCERN_LOCAL,
    READ_CONCERN_MAJORITY       = READ_CONCERN_MAJORITY,
    READ_CONCERN_LINEARIZABLE   = READ_CONCERN_LINEARIZABLE
}
mongo.READ_CONCERN_LOCAL = "READ_CONCERN_LOCAL"
mongo.READ_CONCERN_MAJORITY = "READ_CONCERN_MAJORITY"


local READ_PREFERENCE_PRIMARY = {preference = "primary"} -- only primary
local READ_PREFERENCE_PRIMARYPREFERRED = {preference = "primaryPreferred"} -- prefer primary then secondary
local READ_PREFERENCE_SECONDARY = {preference = "secondary"} -- only secondary
local READ_PREFERENCE_SECONDARYPREFERRED = {preference = "secondaryPreferred"} -- prefer secondary then primary
local READ_PREFERENCE_NEAREST = {preference = "nearest"} -- primary and secondary
local READ_PREFERENCE_MAPPING = {
    READ_PREFERENCE_PRIMARY             = READ_PREFERENCE_PRIMARY,
    READ_PREFERENCE_PRIMARYPREFERRED    = READ_PREFERENCE_PRIMARYPREFERRED,
    READ_PREFERENCE_SECONDARY           = READ_PREFERENCE_SECONDARY,
    READ_PREFERENCE_SECONDARYPREFERRED  = READ_PREFERENCE_SECONDARYPREFERRED,
    READ_PREFERENCE_NEAREST             = READ_PREFERENCE_NEAREST
}
mongo.READ_PREFERENCE_PRIMARY = "READ_PREFERENCE_PRIMARY"
mongo.READ_PREFERENCE_PRIMARYPREFERRED = "READ_PREFERENCE_PRIMARYPREFERRED"
mongo.READ_PREFERENCE_SECONDARY = "READ_PREFERENCE_SECONDARY"
mongo.READ_PREFERENCE_SECONDARYPREFERRED = "READ_PREFERENCE_SECONDARYPREFERRED"
mongo.READ_PREFERENCE_NEAREST = "READ_PREFERENCE_NEAREST"


local UPDATE_ENTRY_MT_READONLY = setmetatable({}, {
    __metatable = "update_entry_mt_readonly_metatable",
    __newindex = function() error("Attempt to set value to UPDATE_ENTRY_MT_READONLY", 2) end,
    __index = function() error("Attempt to get value from UPDATE_ENTRY_MT_READONLY", 2) end
})
local UPDATE_ENTRY_MT = {__metatable = UPDATE_ENTRY_MT_READONLY}
--[[
options:
    [use when in development]:
        upsert
            boolean
            default:false
        multi
            boolean
            default:false
        arrayFilters
            table
            default:nil
--]]
function mongo.gen_update_entry(query, update, options)
    assert(not table.empty(query), "gen_update_entry must take a `query` parameter in not-empty table")
    assert(not table.empty(update), "gen_update_entry must take a `update` parameter in not-empty table")
    local updateEntry = setmetatable({q = query, u = update}, UPDATE_ENTRY_MT)

    options = table.empty(options) and DEFAULT or options

    updateEntry.upsert = not not options.upsert

    updateEntry.multi = not not options.multi

    local arrayFilters = options.arrayFilters
    if not table.empty(arrayFilters) then
        updateEntry.arrayFilters = arrayFilters
    end

    return updateEntry
end


local DELETE_ENTRY_MT_READONLY = setmetatable({}, {
    __metatable = "delete_entry_mt_readonly_metatable",
    __newindex = function() error("Attempt to set value to DELETE_ENTRY_MT_READONLY", 2) end,
    __index = function() error("Attempt to get value from DELETE_ENTRY_MT_READONLY", 2) end
})
local DELETE_ENTRY_MT = {__metatable = DELETE_ENTRY_MT_READONLY}
function mongo.gen_delete_entry(query, limit)
    assert(not table.empty(query), "gen_delete_entry must take a `query` parameter in not-empty table")
    limit = (math.type(limit) == "integer" and limit >= 0) and limit or 1
    local deleteEntry = setmetatable({q = query, limit = limit}, DELETE_ENTRY_MT)

    return deleteEntry
end


local INDEX_ENTRY_MT_READONLY = setmetatable({}, {
    __metatable = "index_entry_mt_readonly_metatable",
    __newindex = function() error("Attempt to set value to INDEX_ENTRY_MT_READONLY", 2) end,
    __index = function() error("Attempt to get value from INDEX_ENTRY_MT_READONLY", 2) end
})
local INDEX_ENTRY_MT = {__metatable = INDEX_ENTRY_MT_READONLY}
--[[
options:
    [use when in development]:
        unique
            boolean
            default:false
        partialFilterExpression
            table
            default:nil
        background
            boolean
            default:false
--]]
function mongo.gen_index_entry(key, name, options)
    assert(not table.empty(key), "gen_index_entry must take a `key` parameter in not-empty table")
    assert(type(name) == "string" and #name>0, "gen_index_entry must take a `name` parameter in not-empty string")
    local indexEntry = setmetatable({key = key, name = name}, INDEX_ENTRY_MT)

    options = table.empty(options) and DEFAULT or options

    indexEntry.unique = not not options.unique

    local partialFilterExpression = options.partialFilterExpression
    if not table.empty(partialFilterExpression) then
        indexEntry.partialFilterExpression = partialFilterExpression
    end

    indexEntry.background = not not options.background

    return indexEntry
end


local function pretty_str_(str)
    if #str == 14 and string.sub(str, 1, 2) == "\x00\x07" then
        local objectid = {"ObjectId(\""}
        for i = 3, 14 do
            table.insert(objectid, string.format("%x", string.byte(str[i]) >> 4))
            table.insert(objectid, string.format("%x", string.byte(str[i]) & 0xf))
        end
        table.insert(objectid, "\")")
        return table.concat(objectid)
    else
        return str
    end
end

local function pretty_(t, level, buff)
    for k, v in pairs(t) do
        if type(k) == "string" then
            k = pretty_str_(k)
        end

        if type(v) == "table" then
            table.insert(buff, string.format("%s%s: {", string.rep("\t", level), k))
            pretty_(v, level+1, buff)
            table.insert(buff, string.format("%s},", string.rep("\t", level)))
        else
            if type(v) == "string" then
                v = pretty_str_(v)
            end
            table.insert(buff, string.format("%s%s: %s,", string.rep("\t", level), k, v))
        end
    end
end

local function pretty_show(t)
    if not USE_PRETTY_SHOW or type(t) ~= "table" then
        return t
    end

    local buff = {"", "{"}
    pretty_(t, 1, buff)
    table.insert(buff, "}")
    table.insert(buff, "")
    return table.concat(buff, "\n")
end


local MongoClient = class()
function MongoClient:ctor(conf)
    self.host = conf.host
    self.port = conf.port or 27017
    self.authmod = conf.authmod
    self.authdb = conf.authdb
    self.username = conf.username
    self.password = conf.password
    self.dbAdmin = self:getDB("admin")

    self.__id = 0
    self.__sock = socketchannel.channel {
        host = self.host,
        port = self.port,
        response = function (fd)
            local len_reply = fd:read(4)
            local reply = fd:read(driver.length(len_reply))
            local result = { result = {} }
            local succ, reply_id, document, cursor_id, startfrom = driver.reply(reply, result.result)
            result.document = document
            result.cursor_id = cursor_id
            result.startfrom = startfrom
            result.data = reply
            return reply_id, succ, result
        end,
        auth = self:auth(),
        nodelay = true,
        overload = conf.overload,
    }
    self.__sock:connect(true) -- try connect only once
end
function MongoClient:tostring()
    return string.format("[MongoClient: %s:%s]", self.host, self.port)
end
mongo.MongoClient = MongoClient


local MongoDB = class()
function MongoDB:ctor(mongoClient, dbname)
    self.connection = mongoClient
    self.name = dbname
    self.full_name = dbname
    self.__cmd = dbname .. ".$cmd"
end
function MongoDB:tostring()
    return string.format("[MongoDB: %s, %s]", self.connection, self.name)
end


local MongoCollection = class()
function MongoCollection:ctor(mongoClient, mongoDB, collectionName)
    self.connection = mongoClient
    self.database = mongoDB
    self.name = collectionName
    self.full_name = mongoDB.full_name .. "." .. collectionName
end
function MongoCollection:tostring()
    return string.format("[MongoCollection: %s, %s, %s]", self.connection, self.database, self.name)
end


local auth_method = {}
function auth_method:auth_mongodb_cr(user, password)
    local r = self:runCommand("getnonce")
    if r.ok ~= 1 then
        return false
    end

    local pass = md5.sumhexa(string.format("%s:mongo:%s", user, password))
    local key = md5.sumhexa(string.format("%s%s%s", r.nonce, user, pass))
    r = self:runCommand("authenticate",1, "user",user, "nonce",r.nonce, "key",key)
    return r.ok == 1
end

local function salt_password(password, salt, iter)
    salt = salt .. "\0\0\0\1"
    local output = crypt.hmac_sha1(password, salt)
    local inter = output
    for _ = 2, iter do
        inter = crypt.hmac_sha1(password, inter)
        output = crypt.xor_str(output, inter)
    end
    return output
end

function auth_method:auth_scram_sha1(username, password)
    local user = string.gsub(string.gsub(username, '=', '=3D'), ',', '=2C')
    local nonce = crypt.base64encode(crypt.randomkey())
    local first_bare = "n="  .. user .. ",r="  .. nonce
    local sasl_start_payload = crypt.base64encode("n,," .. first_bare)

    local r = self:runCommand("saslStart",1, "autoAuthorize",1, "mechanism","SCRAM-SHA-1", "payload",sasl_start_payload)
    if r.ok ~= 1 then
        return false
    end

    local conversationId = r.conversationId
    local server_first = r.payload
    local parsed_s = crypt.base64decode(server_first)
    local parsed_t = {}
    for k, v in string.gmatch(parsed_s, "(%w+)=([^,]*)") do
        parsed_t[k] = v
    end
    local iterations = tonumber(parsed_t['i'])
    local salt = parsed_t['s']
    local rnonce = parsed_t['r']

    if not string.sub(rnonce, 1, 12) == nonce then
        LOG_ERROR("mymongo:auth_method:auth_scram_sha1, server returned an invalid nonce")
        return false
    end

    local without_proof = "c=biws,r=" .. rnonce
    local pbkdf2_key = md5.sumhexa(string.format("%s:mongo:%s", username, password))
    local salted_pass = salt_password(pbkdf2_key, crypt.base64decode(salt), iterations)
    local client_key = crypt.hmac_sha1(salted_pass, "Client Key")
    local stored_key = crypt.sha1(client_key)
    local auth_msg = first_bare .. ',' .. parsed_s .. ',' .. without_proof
    local client_sig = crypt.hmac_sha1(stored_key, auth_msg)
    local client_key_xor_sig = crypt.xor_str(client_key, client_sig)
    local client_proof = "p=" .. crypt.base64encode(client_key_xor_sig)
    local client_final = crypt.base64encode(without_proof .. ',' .. client_proof)
    local server_key = crypt.hmac_sha1(salted_pass, "Server Key")
    local server_sig = crypt.base64encode(crypt.hmac_sha1(server_key, auth_msg))

    r = self:runCommand("saslContinue",1, "conversationId",conversationId, "payload",client_final)
    if r.ok ~= 1 then
        return false
    end

    parsed_s = crypt.base64decode(r.payload)
    parsed_t = {}
    for k, v in string.gmatch(parsed_s, "(%w+)=([^,]*)") do
        parsed_t[k] = v
    end
    if parsed_t['v'] ~= server_sig then
        LOG_ERROR("mymongo:auth_method:auth_scram_sha1, server returned an invalid signature")
        return false
    end

    if not r.done then
        r = self:runCommand("saslContinue",1, "conversationId",conversationId, "payload","")
        if r.ok ~= 1 then
            return false
        end
        if not r.done then
            LOG_ERROR("mymongo:auth_method:auth_scram_sha1, SASL conversation failed to complete")
            return false
        end
    end

    return true
end

local function __parse_addr(addr)
    local host, port = string.match(addr, "([^:]+):(.+)")
    return host, tonumber(port)
end

function MongoClient:auth()
    local user = self.username
    local pass = self.password
    local authmod = "auth_" .. (self.authmod or "scram_sha1")
    local authdb = self:getDB(self.authdb or "admin")

    return function()
        if user ~= nil and pass ~= nil then
            -- autmod can be "mongodb_cr" or "scram_sha1"
            local auth_func = auth_method[authmod]
            assert(auth_func , "Invalid authmod")
            assert(auth_func(authdb, user, pass))
        end

        local rs_data = self:runCommand("isMaster")
        if rs_data.ok == 1 then
            if rs_data.hosts then
                local backup = {}
                for _, v in ipairs(rs_data.hosts) do
                    local host, port = __parse_addr(v)
                    table.insert(backup, {host = host, port = port})
                end
                self.__sock:changebackup(backup)
            end
            if rs_data.ismaster then
                self.__pickserver = nil
                return
            else
                if rs_data.primary then
                    local host, port = __parse_addr(rs_data.primary)
                    self.host = host
                    self.port = port
                    self.__sock:changehost(host, port)
                else
                    LOG_ERROR("mymongo:MongoClient:auth, no primary return", {me=rs_data.me})
                    -- determine the primary db using hosts
                    local pickserver = {}
                    if self.__pickserver == nil then
                        for _, v in ipairs(rs_data.hosts) do
                            if v ~= rs_data.me then
                                table.insert(pickserver, v)
                            end
                        end
                        self.__pickserver = pickserver
                    end

                    if #self.__pickserver <= 0 then
                        error("mymongo:MongoClient:auth, can not determine the primary db")
                    end

                    LOG_ERROR("mymongo:MongoClient:auth, try to connect", {address=self.__pickserver[1]})
                    local host, port = __parse_addr(self.__pickserver[1])
                    table.remove(self.__pickserver, 1)
                    self.host = host
                    self.port = port
                    self.__sock:changehost(host, port)
                end
            end
        end
    end
end

function MongoClient:getDB(dbname)
    return MongoDB.new(self, dbname)
end

function MongoClient:disconnect()
    if self.__sock then
        local fd = self.__sock
        self.__sock = false
        fd:close()
    end
end

function MongoClient:genId()
    local id = self.__id + 1
    self.__id = id
    return id
end

function MongoClient:runCommand(...)
    return self.dbAdmin:runCommand(...)
end

function MongoClient:logout()
    local r = self:runCommand("logout")
    return r.ok == 1
end


function MongoDB:runCommand(cmd, cmd_v, ...)
    local conn = self.connection
    local request_id = conn:genId()
    local sock = conn.__sock
    local bson_cmd
    if not cmd_v then
        bson_cmd = bson_encode_order(cmd, 1)
    else
        bson_cmd = bson_encode_order(cmd, cmd_v, ...)
    end
    local pack = driver.query(request_id, 0, self.__cmd, 0, 1, bson_cmd)
    -- we must hold req (req.data), because req.document is a lightuserdata, it's a pointer to the string (req.data)
    local req = sock:request(pack, request_id)
    local doc = req.document
    return bson_decode(doc)
end

function MongoDB:getCollection(collectionName)
    return MongoCollection.new(self.connection, self, collectionName)
end


--[[
{
   insert: <collection>,
   documents: [ <document>, <document>, <document>, ... ],
   ordered: <boolean>,
   writeConcern: { <write concern> },
   bypassDocumentValidation: <boolean>
}

insert(string)
    The name of the target collection.
documents(array)
    An array of one or more documents to insert into the named collection.
ordered(boolean)Optional.
    If true, then when an insert of a document fails, return without inserting any remaining documents listed in the inserts array.
    If false, then when an insert of a document fails, continue to insert the remaining documents. Defaults to true.
writeConcern(document)Optional.
    A document that expresses the write concern of the insert command. Omit to use the default write concern.
    Do not explicitly set the write concern for the operation if run in a transaction.
    To use write concern with transactions, see Read Concern/Write Concern/Read Preference.
bypassDocumentValidation(boolean)Optional.
    Enables insert to bypass document validation during the operation.
    This lets you insert documents that do not meet the validation requirements.
    New in version 3.2.
--]]
local function insert_(self, docs, options)
    for _, doc in ipairs(docs) do
        if doc._id == nil then
            doc._id = bson.objectid()
        end
    end
    local cmd = {"insert",self.name, "documents",docs}

    local ordered = options.ordered
    if ordered ~= nil then
        table.insert(cmd, "ordered")
        table.insert(cmd, not not ordered)
    end

    local wtimeout = options.wtimeout
    if math.type(wtimeout) ~= "integer" or wtimeout <= 0 then
        wtimeout = 30000
    end
    local writeConcern = WRITE_CONCERN_MAPPING[options.writeConcern]
    if not writeConcern then
        writeConcern = WRITE_CONCERN_MAJORITY
    end
    table.insert(cmd, "writeConcern")
    table.insert(cmd, {w = writeConcern.concern, j = writeConcern.journal, wtimeout = wtimeout})

    local token = OPERATION_TOKEN
    OPERATION_TOKEN = OPERATION_TOKEN + 1
    LOG_DEBUG("MongoCollection:insert_, request",  {token=token, collection=self.name, cmd=pretty_show(cmd)})
    local success, r = pcall(self.database.runCommand, self.database, table.unpack(cmd))
    LOG_DEBUG("MongoCollection:insert_, response", {token=token, collection=self.name, success=success, r=r})
    if not success then
        return false, r
    else
        if r.ok == 1 and not r.writeErrors and not r.writeConcernError then
            return true, r.n
        else
            if r.errmsg then
                return false, r.errmsg
            elseif r.writeErrors then
                local errs = {}
                for _, err in ipairs(r.writeErrors) do
                    table.insert(errs, err.errmsg)
                end
                return false, table.concat(errs, '\n')
            else
                return false, r.writeConcernError.errmsg
            end
        end
    end
end

--[[
options:
    [use when you know what it is]:
        wtimeout
            >0
            default:30000
        writeConcern
            WRITE_CONCERN_PRIMARY|WRITE_CONCERN_MAJORITY
            default:WRITE_CONCERN_MAJORITY
--]]
function MongoCollection:insert_one(doc, options)
    assert(not table.empty(doc), "insert_one must take a `doc` parameter in not-empty table")
    options = table.empty(options) and DEFAULT or options
    return insert_(self,{doc}, options)
end

--[[
options:
    [use when you know what it is]:
        ordered
            boolean
            default:true
        wtimeout
            >0
            default:30000
        writeConcern
            WRITE_CONCERN_PRIMARY|WRITE_CONCERN_MAJORITY
            default:WRITE_CONCERN_MAJORITY
--]]
function MongoCollection:insert_bulk(docs, options)
    assert(not table.empty(docs), "insert_bulk must take a `docs` parameter in not-empty table")
    for _, doc in ipairs(docs) do
        assert(not table.empty(doc), "insert_bulk must take a `docs` parameter which contains not-empty table")
    end
    options = table.empty(options) and DEFAULT or options
    return insert_(self, docs, options)
end


--[[
{
   "find": <string>,
   "filter": <document>,
   "sort": <document>,
   "projection": <document>,
   "hint": <document or string>,
   "skip": <int>,
   "limit": <int>,
   "batchSize": <int>,
   "singleBatch": <bool>,
   "comment": <string>,
   "maxTimeMS": <int>,
   "readConcern": <document>,
   "max": <document>,
   "min": <document>,
   "returnKey": <bool>,
   "showRecordId": <bool>,
   "tailable": <bool>,
   "oplogReplay": <bool>,
   "noCursorTimeout": <bool>,
   "awaitData": <bool>,
   "allowPartialResults": <bool>,
   "collation": <document>
}

find(string)
    The name of the collection or view to query.
filter(document)Optional.
    The query predicate. If unspecified, then all documents in the collection will match the predicate.
sort(document)Optional.
    The sort specification for the ordering of the results.
projection(document)Optional.
    The projection specification to determine which fields to include in the returned documents.
    See Project Fields to Return from Query and Projection Operators.
hint(string or document)Optional.
    Index specification. Specify either the index name as a string or the index key pattern.
    If specified, then the query system will only consider plans using the hinted index.
skip(Positive integer)Optional.
    Number of documents to skip. Defaults to 0.
limit(Non-negative integer)Optional.
    The maximum number of documents to return.
    If unspecified, then defaults to no limit. A limit of 0 is equivalent to setting no limit.
batchSize(non-negative integer)Optional.
    The number of documents to return in the first batch. Defaults to 101.
    A batchSize of 0 means that the cursor will be established, but no documents will be returned in the first batch.
    Unlike the previous wire protocol version, a batchSize of 1 for the find command does not close the cursor.
singleBatch(boolean)Optional.
    Determines whether to close the cursor after the first batch. Defaults to false.
comment(string)Optional.
    A comment to attach to the query to help interpret and trace query profile data.
maxTimeMS(positive integer)Optional.
    The cumulative time limit in milliseconds for processing operations on the cursor.
    MongoDB aborts the operation at the earliest following interrupt point.
readConcern(document)Optional.
    Specifies the read concern.
    The readConcern option has the following syntax:
    Changed in version 3.6.
    readConcern: { level: <value> }
    Possible read concern levels are:
        "local". This is the default read concern level.
        "available". This is the default for reads against secondaries when Read Operations and afterClusterTime and “level” are unspecified. The query returns the instance’s most recent data.
        "majority". Available for replica sets that use WiredTiger storage engine.
        "linearizable". Available for read operations on the primary only.
    For more formation on the read concern levels, see Read Concern Levels.
    For "local" (default) or "majority" read concern level, you can specify the afterClusterTime option to have the read operation return data that meets the level requirement and the specified after cluster time requirement.
    For more information, see Read Operations and afterClusterTime.
    The getMore command uses the readConcern level specified in the originating find command.
max(document)Optional.
    The exclusive upper bound for a specific index. See cursor.max() for details.
min(document)Optional.
    The inclusive lower bound for a specific index. See cursor.min() for details.
returnKey(boolean)Optional.
    If true, returns only the index keys in the resulting documents. Default value is false.
    If returnKey is true and the find command does not use an index, the returned documents will be empty.
showRecordId(boolean)Optional.
    Determines whether to return the record identifier for each document. If true, adds a field $recordId to the returned documents.
tailable(boolean)Optional.
    Returns a tailable cursor for a capped collections.
awaitData(boolean)Optional.
    Use in conjunction with the tailable option to block a getMore command on the cursor temporarily if at the end of data rather than returning no data.
    After a timeout period, find returns as normal.
oplogReplay(boolean)Optional.
    An internal command for replaying a replica set’s oplog.
    To use oplogReplay, the find field must refer to a capped collection and you must provide a filter option comparing the ts document field to a timestamp using one of the following comparison operators:
        $gte
        $gt
        $eq
noCursorTimeout(boolean)Optional.
    Prevents the server from timing out idle cursors after an inactivity period (10 minutes).
allowPartialResults(boolean)Optional.
    For queries against a sharded collection, returns partial results from the mongos if some shards are unavailable instead of throwing an error.
collation(document)Optional.
    Specifies the collation to use for the operation.
    Collation allows users to specify language-specific rules for string comparison, such as rules for lettercase and accent marks.
    The collation option has the following syntax:
        collation: {
           locale: <string>,
           caseLevel: <boolean>,
           caseFirst: <string>,
           strength: <int>,
           numericOrdering: <boolean>,
           alternate: <string>,
           maxVariable: <string>,
           backwards: <boolean>
        }
    When specifying collation, the locale field is mandatory; all other collation fields are optional.
    For descriptions of the fields, see Collation Document.
    If the collation is unspecified but the collection has a default collation (see db.createCollection()), the operation uses the collation specified for the collection.
    If no collation is specified for the collection or for the operations, MongoDB uses the simple binary comparison used in prior versions for string comparisons.
    You cannot specify multiple collations for an operation.
    For example, you cannot specify different collations per field, or if performing a find with a sort, you cannot use one collation for the find and another for the sort.
    New in version 3.4.
--]]
local function find_(self, query, options)
    local cmd = {"find",self.name, "filter",query}

    local projection = options.projection
    if not table.empty(projection) then
        table.insert(cmd, "projection")
        table.insert(cmd, projection)
    end

    local sort = options.sort
    if not table.empty(sort) then
        table.insert(cmd, "sort")
        table.insert(cmd, sort)
    end

    local skip = options.skip
    if math.type(skip) == "integer" and skip > 0 then
        table.insert(cmd, "skip")
        table.insert(cmd, skip)
    end

    local limit = options.limit
    if math.type(limit) == "integer" and limit >= 0 then
        table.insert(cmd, "limit")
        table.insert(cmd, limit)
    end

    local hint = options.hint
    if type(hint) == "string" or not table.empty(hint) then
        table.insert(cmd, "hint")
        table.insert(cmd, hint)
    end

    local batchSize = options.batchSize
    if math.type(batchSize) == "integer" and batchSize >= 0 then
        table.insert(cmd, "batchSize")
        table.insert(cmd, batchSize)
    end

    local singleBatch = options.singleBatch
    if singleBatch ~= nil then
        table.insert(cmd, "singleBatch")
        table.insert(cmd, not not singleBatch)
    end

    local maxTimeMS = options.maxTimeMS
    if math.type(maxTimeMS) ~= "integer" or maxTimeMS <= 0 then
        maxTimeMS = 30000
    end
    table.insert(cmd, "maxTimeMS")
    table.insert(cmd, maxTimeMS)

    local readConcern = READ_CONCERN_MAPPING[options.readConcern]
    if not readConcern then
        readConcern = READ_CONCERN_MAJORITY
    end
    table.insert(cmd, "readConcern")
    table.insert(cmd, {level = readConcern.concern})

    local readPreference = READ_PREFERENCE_MAPPING[options.readPreference]
    if not readPreference then
        readPreference = READ_PREFERENCE_SECONDARYPREFERRED
    end
    if readConcern == READ_CONCERN_LINEARIZABLE then
        readPreference = READ_PREFERENCE_PRIMARY
    end
    table.insert(cmd, "$readPreference")
    table.insert(cmd, {mode = readPreference.preference})

    local allowPartialResults = options.allowPartialResults
    if allowPartialResults == nil then
        allowPartialResults = true
    end
    table.insert(cmd, "allowPartialResults")
    table.insert(cmd, not not allowPartialResults)

    local token = OPERATION_TOKEN
    OPERATION_TOKEN = OPERATION_TOKEN + 1
    LOG_DEBUG("MongoCollection:find_, request",  {token=token, collection=self.name, cmd=pretty_show(cmd)})
    local success, r = pcall(self.database.runCommand, self.database, table.unpack(cmd))
    LOG_DEBUG("MongoCollection:find_, response", {token=token, collection=self.name, success=success, r=r})
    if not success then
        return false, r
    else
        if r.ok == 1 and not r.readErrors and not r.readConcernError then
            return true, r.cursor.firstBatch, r.cursor.id
        else
            if r.errmsg then
                return false, r.errmsg
            elseif r.readErrors then
                local errs = {}
                for _, err in ipairs(r.readErrors) do
                    table.insert(errs, err.errmsg)
                end
                return false, table.concat(errs, '\n')
            else
                return false, r.readConcernError.errmsg
            end
        end
    end
end

--[[
options:
    [use when in development]:
        sort
            table
            {xxx:1, xxx:-1, xxx:1}
            default:nil
        skip
            >0
            default:0
    [use when you know what it is]:
        hint
            string|table
            xxx|{xxx:1, xxx:-1, xxx:1}
            default:nil
        batchSize
            >=0
            default:101
        singleBatch
            boolean
            default:false
        maxTimeMS
            >0
            default:30000
        readConcern
            READ_CONCERN_LOCAL|READ_CONCERN_MAJORITY|READ_CONCERN_LINEARIZABLE
            default:READ_CONCERN_MAJORITY
        readPreference
            READ_PREFERENCE_PRIMARY|READ_PREFERENCE_SECONDARY|READ_PREFERENCE_PRIMARYPREFERRED|READ_PREFERENCE_SECONDARYPREFERRED|READ_PREFERENCE_NEAREST
            default:READ_PREFERENCE_SECONDARYPREFERRED
        allowPartialResults
            boolean
            default:true
--]]
function MongoCollection:find_one(query, filter, options)
    assert(not table.empty(query), "find_one must take a `query` parameter in not-empty table")
    options = table.empty(options) and {} or table.deepcopy(options, true)
    options.projection = filter
    options.limit = 1
    local success, firstBatch, _ = find_(self, query, options)
    if not success then
        return success, firstBatch
    else
        return success, not table.empty(firstBatch) and firstBatch[1] or nil
    end
end

--[[
function MongoCollection:find_one_linearize(query, filter, options)
    assert(not table.empty(query), "find_one_linearize must take a `query` parameter in not-empty table")
    options = table.empty(options) and {} or table.deepcopy(options, true)
    options.readConcern = "READ_CONCERN_LINEARIZABLE"
    return self:find_one(query, filter, options)
end
--]]

--[[
options:
    [use when in development]:
        sort
            table
            {xxx:1, xxx:-1, xxx:1}
            default:nil
        skip
            >0
            default:0
        limit
            >=0
            default:10
    [use when you know what it is]:
        hint
            string|table
            xxx|{xxx:1, xxx:-1, xxx:1}
            default:nil
        batchSize
            >=0
            default:101
        singleBatch
            boolean
            default:false
        maxTimeMS
            >0
            default:30000
        readConcern
            READ_CONCERN_LOCAL|READ_CONCERN_MAJORITY
            default:READ_CONCERN_MAJORITY
        readPreference
            READ_PREFERENCE_PRIMARY|READ_PREFERENCE_SECONDARY|READ_PREFERENCE_PRIMARYPREFERRED|READ_PREFERENCE_SECONDARYPREFERRED|READ_PREFERENCE_NEAREST
            default:READ_PREFERENCE_SECONDARYPREFERRED
        allowPartialResults
            boolean
            default:true
--]]
function MongoCollection:find_many(query, filter, options)
    assert(not table.empty(query), "find_many must take a `query` parameter in not-empty table")
    options = table.empty(options) and {} or table.deepcopy(options, true)
    options.projection = filter
    local limit = options.limit
    options.limit = (math.type(limit) == "integer" and limit >= 0) and limit or 10
    return find_(self, query, options)
end


--[[
{
   "getMore": <long>,
   "collection": <string>,
   "batchSize": <int>,
   "maxTimeMS": <int>
}

getMore(long)
    The cursor id.
collection(string)
    The name of the collection over which the cursor is operating.
batchSize(positive integer)Optional.
    The number of documents to return in the batch.
maxTimeMS(non-negative integer)Optional.
    Specifies a time limit in milliseconds for processing operations on a cursor.
    If you do not specify a value for maxTimeMS, operations will not time out.
    A value of 0 explicitly specifies the default unbounded behavior.
    MongoDB terminates operations that exceed their allotted time limit using the same mechanism as db.killOp().
    MongoDB only terminates an operation at one of its designated interrupt points.
--]]
local function more_(self, cursorId, options)
    local cmd = {"getMore",cursorId, "collection",self.name}

    local batchSize = options.batchSize
    if math.type(batchSize) == "integer" and batchSize > 0 then
        table.insert(cmd, "batchSize")
        table.insert(cmd, batchSize)
    end

    local maxTimeMS = options.maxTimeMS
    if math.type(maxTimeMS) == "integer" and maxTimeMS > 0 then
        table.insert(cmd, "maxTimeMS")
        table.insert(cmd, maxTimeMS)
    end

    local token = OPERATION_TOKEN
    OPERATION_TOKEN = OPERATION_TOKEN + 1
    LOG_DEBUG("MongoCollection:more_, request",  {token=token, collection=self.name, cmd=pretty_show(cmd)})
    local success, r = pcall(self.database.runCommand, self.database, table.unpack(cmd))
    LOG_DEBUG("MongoCollection:more_, response", {token=token, collection=self.name, success=success, r=r})
    if not success then
        return false, r
    else
        if r.ok == 1 and not r.readErrors and not r.readConcernError then
            return true, r.cursor.nextBatch, r.cursor.id
        else
            if r.errmsg then
                return false, r.errmsg
            elseif r.readErrors then
                local errs = {}
                for _, err in ipairs(r.readErrors) do
                    table.insert(errs, err.errmsg)
                end
                return false, table.concat(errs, '\n')
            else
                return false, r.readConcernError.errmsg
            end
        end
    end
end

--[[
options:
    [use when you know what it is]:
        batchSize
            >0
            default:101
        maxTimeMS
            >=0
            default:0
--]]
function MongoCollection:get_more(cursorId, options)
    assert(math.type(cursorId) == "integer", "get_more must take an integer `cursorId`")
    options = table.empty(options) and DEFAULT or options
    return more_(self, cursorId, options)
end


--[[
{
   update: <collection>,
   updates: [
      { q: <query>, u: <update>, upsert: <boolean>, multi: <boolean>,
        collation: <document>, arrayFilters: <array> },
      { q: <query>, u: <update>, upsert: <boolean>, multi: <boolean>,
        collation: <document>, arrayFilters: <array> },
      { q: <query>, u: <update>, upsert: <boolean>, multi: <boolean>,
        collation: <document>, arrayFilters: <array> },
      ...
   ],
   ordered: <boolean>,
   writeConcern: { <write concern> },
   bypassDocumentValidation: <boolean>
}

update(string)
    The name of the target collection.
updates(array)
    An array of one or more update statements to perform in the named collection.
ordered(boolean)Optional.
    If true, then when an update statement fails, return without performing the remaining update statements. If false, then when an update fails, continue with the remaining update statements, if any. Defaults to true.
writeConcern(document)Optional.
    A document expressing the write concern of the update command. Omit to use the default write concern.
    Do not explicitly set the write concern for the operation if run in a transaction.
    To use write concern with transactions, see Read Concern/Write Concern/Read Preference.
bypassDocumentValidation(boolean)Optional.
    Enables update to bypass document validation during the operation.
    This lets you update documents that do not meet the validation requirements.
    New in version 3.2.

q(document)
    The query that matches documents to update. Use the same query selectors as used in the find() method.
u(document)
    The modifications to apply. For details, see Behavior.
upsert(boolean)Optional.
    If true, perform an insert if no documents match the query.
    If both upsert and multi are true and no documents match the query, the update operation inserts only a single document.
multi(boolean)Optional.
    If true, updates all documents that meet the query criteria.
    If false, limit the update to one document that meet the query criteria.
    Defaults to false.

collation(document)Optional.
    Specifies the collation to use for the operation.
    Collation allows users to specify language-specific rules for string comparison, such as rules for lettercase and accent marks.
    The collation option has the following syntax:
        collation: {
           locale: <string>,
           caseLevel: <boolean>,
           caseFirst: <string>,
           strength: <int>,
           numericOrdering: <boolean>,
           alternate: <string>,
           maxVariable: <string>,
           backwards: <boolean>
        }
    When specifying collation, the locale field is mandatory; all other collation fields are optional.
    For descriptions of the fields, see Collation Document.
    If the collation is unspecified but the collection has a default collation (see db.createCollection()), the operation uses the collation specified for the collection.
    If no collation is specified for the collection or for the operations, MongoDB uses the simple binary comparison used in prior versions for string comparisons.
    You cannot specify multiple collations for an operation.
    For example, you cannot specify different collations per field, or if performing a find with a sort, you cannot use one collation for the find and another for the sort.
    New in version 3.4.
arrayFilters(array)Optional.
    An array of filter documents that determines which array elements to modify for an update operation on an array field.
    In the update document, use the $[<identifier>] filtered positional operator to define an identifier, which you then reference in the array filter documents.
    You cannot have an array filter document for an identifier if the identifier is not included in the update document.
    The <identifier> must begin with a lowercase letter and contain only alphanumeric characters.
    You can include the same identifier multiple times in the update document
--]]
local function update_(self, updateEntries, options)
    local cmd = {"update",self.name, "updates",updateEntries}

    local ordered = options.ordered
    if ordered ~= nil then
        table.insert(cmd, "ordered")
        table.insert(cmd, not not ordered)
    end

    local wtimeout = options.wtimeout
    if math.type(wtimeout) ~= "integer" or wtimeout <= 0 then
        wtimeout = 30000
    end
    local writeConcern = WRITE_CONCERN_MAPPING[options.writeConcern]
    if not writeConcern then
        writeConcern = WRITE_CONCERN_MAJORITY
    end
    table.insert(cmd, "writeConcern")
    table.insert(cmd, {w = writeConcern.concern, j = writeConcern.journal, wtimeout = wtimeout})

    local token = OPERATION_TOKEN
    OPERATION_TOKEN = OPERATION_TOKEN + 1
    LOG_DEBUG("MongoCollection:update_, request",  {token=token, collection=self.name, cmd=pretty_show(cmd)})
    local success, r = pcall(self.database.runCommand, self.database, table.unpack(cmd))
    LOG_DEBUG("MongoCollection:update_, response", {token=token, collection=self.name, success=success, r=r})
    if not success then
        return false, r
    else
        if r.ok == 1 and not r.writeErrors and not r.writeConcernError then
            return true, r.n, r.nModified, r.upserted
        else
            if r.errmsg then
                return false, r.errmsg
            elseif r.writeErrors then
                local errs = {}
                for _, err in ipairs(r.writeErrors) do
                    table.insert(errs, err.errmsg)
                end
                return false, table.concat(errs, '\n')
            else
                return false, r.writeConcernError.errmsg
            end
        end
    end
end

--[[
options:
    [use when in development]:
        upsert
            boolean
            default:false
        arrayFilters
            table
            default:nil
    [use when you know what it is]:
        wtimeout
            >0
            default:30000
        writeConcern
            WRITE_CONCERN_PRIMARY|WRITE_CONCERN_MAJORITY
            default:WRITE_CONCERN_MAJORITY
--]]
function MongoCollection:update_one(query, update, options)
    assert(not table.empty(query), "update_one must take a `query` parameter in not-empty table")
    assert(not table.empty(update), "update_one must take a `update` parameter in not-empty table")
    options = table.empty(options) and DEFAULT or options
    return update_(self, {mongo.gen_update_entry(query, update, {
        upsert = not not options.upsert,
        multi = false,
        arrayFilters = not table.empty(options.arrayFilters) and options.arrayFilters or nil
    })}, options)
end

--[[
options:
    [use when in development]:
        upsert
            boolean
            default:false
        arrayFilters
            table
            default:nil
    [use when you know what it is]:
        wtimeout
            >0
            default:30000
        writeConcern
            WRITE_CONCERN_PRIMARY|WRITE_CONCERN_MAJORITY
            default:WRITE_CONCERN_MAJORITY
--]]
function MongoCollection:update_many(query, update, options)
    assert(not table.empty(query), "update_many must take a `query` parameter in not-empty table")
    assert(not table.empty(update), "update_many must take a `update` parameter in not-empty table")
    options = table.empty(options) and DEFAULT or options
    return update_(self, {mongo.gen_update_entry(query, update, {
        upsert = not not options.upsert,
        multi = true,
        arrayFilters = not table.empty(options.arrayFilters) and options.arrayFilters or nil
    })}, options)
end

--[[
options:
    [use when you know what it is]:
        ordered
            boolean
            default:true
        wtimeout
            >0
            default:30000
        writeConcern
            WRITE_CONCERN_PRIMARY|WRITE_CONCERN_MAJORITY
            default:WRITE_CONCERN_MAJORITY
--]]
function MongoCollection:update_bulk(updateEntries, options)
    assert(not table.empty(updateEntries), "update_bulk must take a `updateEntries` parameter in not-empty table")
    for _, updateEntry in ipairs(updateEntries) do
        assert(getmetatable(updateEntry) == UPDATE_ENTRY_MT_READONLY, "update_bulk must take a `updateEntries` parameter which contains elements with UPDATE_ENTRY_MT")
    end
    options = table.empty(options) and DEFAULT or options
    return update_(self, updateEntries, options)
end


--[[
{
  findAndModify: <collection-name>,
  query: <document>,
  sort: <document>,
  remove: <boolean>,
  update: <document>,
  new: <boolean>,
  fields: <document>,
  upsert: <boolean>,
  bypassDocumentValidation: <boolean>,
  writeConcern: <document>,
  collation: <document>,
  arrayFilters: <array>
}

query(document)Optional.
    The selection criteria for the modification.
    The query field employs the same query selectors as used in the db.collection.find() method.
    Although the query may match multiple documents, findAndModify will only select one document to modify.
    If unspecified, defaults to an empty document.
    Starting in MongoDB 4.0.12+ (and 3.6.14+ and 3.4.23+), the operation errors if the query argument is not a document.
sort(document)Optional.
    Determines which document the operation modifies if the query selects multiple documents.
    findAndModify modifies the first document in the sort order specified by this argument.
    Starting in MongoDB 4.0.12+ (and 3.6.14+ and 3.4.23+), the operation errors if the sort argument is not a document.
remove(boolean)
    Must specify either the remove or the update field. Removes the document specified in the query field.
    Set this to true to remove the selected document .
    The default is false.
update(document)
    Must specify either the remove or the update field.
    Performs an update of the selected document.
    The update field employs the same update operators or field: value specifications to modify the selected document.
new(boolean)Optional.
    When true, returns the modified document rather than the original.
    The findAndModify method ignores the new option for remove operations.
    The default is false.
fields(document)Optional.
    A subset of fields to return. The fields document specifies an inclusion of a field with 1, as in: fields: { <field1>: 1, <field2>: 1, ... }. See projection.
    Starting in MongoDB 4.0.12+ (and 3.6.14+ and 3.4.23+), the operation errors if the fields argument is not a document.
upsert(boolean)Optional.
    Used in conjuction with the update field.
    When true, findAndModify() either:
        Creates a new document if no documents match the query. For more details see upsert behavior.
        Updates a single document that matches the query.
    To avoid multiple upserts, ensure that the query fields are uniquely indexed.
    Defaults to false.
bypassDocumentValidation(boolean)Optional.
    Enables findAndModify to bypass document validation during the operation.
    This lets you update documents that do not meet the validation requirements.
    New in version 3.2.
writeConcern(document)Optional.
    A document expressing the write concern. Omit to use the default write concern.
    Do not explicitly set the write concern for the operation if run in a transaction.
    To use write concern with transactions, see Read Concern/Write Concern/Read Preference.
    New in version 3.2.
maxTimeMS(integer)Optional.
    Specifies a time limit in milliseconds for processing the operation.
findAndModify(string)
    The collection against which to run the command.
collation(document)Optional
    Specifies the collation to use for the operation.
    Collation allows users to specify language-specific rules for string comparison, such as rules for lettercase and accent marks.
    The collation option has the following syntax:
        collation: {
           locale: <string>,
           caseLevel: <boolean>,
           caseFirst: <string>,
           strength: <int>,
           numericOrdering: <boolean>,
           alternate: <string>,
           maxVariable: <string>,
           backwards: <boolean>
        }
    When specifying collation, the locale field is mandatory; all other collation fields are optional. For descriptions of the fields, see Collation Document.
    If the collation is unspecified but the collection has a default collation (see db.createCollection()), the operation uses the collation specified for the collection.
    If no collation is specified for the collection or for the operations, MongoDB uses the simple binary comparison used in prior versions for string comparisons.
    You cannot specify multiple collations for an operation.
    For example, you cannot specify different collations per field, or if performing a find with a sort, you cannot use one collation for the find and another for the sort.
    New in version 3.4.
arrayFilters(array)Optional.
    An array of filter documents that determines which array elements to modify for an update operation on an array field.
    In the update document, use the $[<identifier>] filtered positional operator to define an identifier, which you then reference in the array filter documents.
    You cannot have an array filter document for an identifier if the identifier is not included in the update document.
    The <identifier> must begin with a lowercase letter and contain only alphanumeric characters.
    You can include the same identifier multiple times in the update document
--]]
local function find_and_modify_(self, query, options)
    local cmd = {"findAndModify",self.name, "query",query}

    local sort = options.sort
    if not table.empty(sort) then
        table.insert(cmd, "sort")
        table.insert(cmd, sort)
    end

    local fields = options.fields
    if not table.empty(fields) then
        table.insert(cmd, "fields")
        table.insert(cmd, fields)
    end

    if options.remove then
        table.insert(cmd, "remove")
        table.insert(cmd, true)
    else
        table.insert(cmd, "update")
        table.insert(cmd, options.update)

        table.insert(cmd, "new")
        table.insert(cmd, not not options.new)

        table.insert(cmd, "upsert")
        table.insert(cmd, not not options.upsert)

        local arrayFilters = options.arrayFilters
        if not table.empty(arrayFilters) then
            table.insert(cmd, "arrayFilters")
            table.insert(cmd, arrayFilters)
        end
    end

    local maxTimeMS = options.maxTimeMS
    if math.type(maxTimeMS) ~= "integer" or maxTimeMS <= 0 then
        maxTimeMS = 30000
    end
    table.insert(cmd, "maxTimeMS")
    table.insert(cmd, maxTimeMS)

    local writeConcern = WRITE_CONCERN_MAPPING[options.writeConcern]
    if not writeConcern then
        writeConcern = WRITE_CONCERN_MAJORITY
    end
    table.insert(cmd, "writeConcern")
    table.insert(cmd, {w = writeConcern.concern, j = writeConcern.journal, wtimeout = maxTimeMS})

    local token = OPERATION_TOKEN
    OPERATION_TOKEN = OPERATION_TOKEN + 1
    LOG_DEBUG("MongoCollection:find_and_modify_, request",  {token=token, collection=self.name, cmd=pretty_show(cmd)})
    local success, r = pcall(self.database.runCommand, self.database, table.unpack(cmd))
    LOG_DEBUG("MongoCollection:find_and_modify_, response", {token=token, collection=self.name, success=success, r=r})
    if not success then
        return false, r
    else
        if r.ok == 1 then
            return true, type(r.value)=="table" and r.value or nil, r.lastErrorObject.updatedExisting, r.lastErrorObject.upserted
        else
            return false, r.errmsg
        end
    end
end

--[[
options:
    [use when in development]:
        sort
            table
            {xxx:1, xxx:-1, xxx:1}
            default:nil
        new
            boolean
            default:false
        upsert
            boolean
            default:false
        arrayFilters
            table
            default:nil
    [use when you know what it is]:
        maxTimeMS
            >0
            default:30000
        writeConcern
            WRITE_CONCERN_PRIMARY|WRITE_CONCERN_MAJORITY
            default:WRITE_CONCERN_MAJORITY
--]]
function MongoCollection:find_and_update(query, update, filter, options)
    assert(not table.empty(query), "find_and_update must take a `query` parameter in not-empty table")
    assert(not table.empty(update), "find_and_update must take a `update` parameter in not-empty table")
    options = table.empty(options) and {} or table.deepcopy(options, true)
    options.remove = false
    options.update = update
    options.fields = filter
    return find_and_modify_(self, query, options)
end

--[[
options:
    [use when in development]:
        sort
            table
            {xxx:1, xxx:-1, xxx:1}
            default:nil
    [use when you know what it is]:
        maxTimeMS
            >0
            default:30000
        writeConcern
            WRITE_CONCERN_PRIMARY|WRITE_CONCERN_MAJORITY
            default:WRITE_CONCERN_MAJORITY
--]]
function MongoCollection:find_and_remove(query, filter, options)
    assert(not table.empty(query), "find_and_remove must take a `query` parameter in not-empty table")
    options = table.empty(options) and {} or table.deepcopy(options, true)
    options.remove = true
    options.fields = filter
    return find_and_modify_(self, query, options)
end


--[[
{
   delete: <collection>,
   deletes: [
      { q : <query>, limit : <integer>, collation: <document> },
      { q : <query>, limit : <integer>, collation: <document> },
      { q : <query>, limit : <integer>, collation: <document> },
      ...
   ],
   ordered: <boolean>,
   writeConcern: { <write concern> }
}

delete(string)
    The name of the target collection.
deletes(array)
    An array of one or more delete statements to perform in the named collection.
ordered(boolean)Optional.
    If true, then when a delete statement fails, return without performing the remaining delete statements.
    If false, then when a delete statement fails, continue with the remaining delete statements, if any. Defaults to true.
writeConcern(document)Optional.
    A document expressing the write concern of the delete command. Omit to use the default write concern.
    Do not explicitly set the write concern for the operation if run in a transaction.
    To use write concern with transactions, see Read Concern/Write Concern/Read Preference.

q(document)
    The query that matches documents to delete.
limit(integer)
    The number of matching documents to delete.
    Specify either a 0 to delete all matching documents or 1 to delete a single document.
collation(document)Optional.
    Specifies the collation to use for the operation.
    Collation allows users to specify language-specific rules for string comparison, such as rules for lettercase and accent marks.
    The collation option has the following syntax:
        collation: {
           locale: <string>,
           caseLevel: <boolean>,
           caseFirst: <string>,
           strength: <int>,
           numericOrdering: <boolean>,
           alternate: <string>,
           maxVariable: <string>,
           backwards: <boolean>
        }
    When specifying collation, the locale field is mandatory; all other collation fields are optional. For descriptions of the fields, see Collation Document.
    If the collation is unspecified but the collection has a default collation (see db.createCollection()), the operation uses the collation specified for the collection.
    If no collation is specified for the collection or for the operations, MongoDB uses the simple binary comparison used in prior versions for string comparisons.
    You cannot specify multiple collations for an operation.
    For example, you cannot specify different collations per field, or if performing a find with a sort, you cannot use one collation for the find and another for the sort.
    New in version 3.4.
--]]
local function delete_(self, deleteEntries, options)
    local cmd = {"delete",self.name, "deletes",deleteEntries}

    local ordered = options.ordered
    if ordered ~= nil then
        table.insert(cmd, "ordered")
        table.insert(cmd, not not ordered)
    end

    local wtimeout = options.wtimeout
    if math.type(wtimeout) ~= "integer" or wtimeout <= 0 then
        wtimeout = 30000
    end
    local writeConcern = WRITE_CONCERN_MAPPING[options.writeConcern]
    if not writeConcern then
        writeConcern = WRITE_CONCERN_MAJORITY
    end
    table.insert(cmd, "writeConcern")
    table.insert(cmd, {w = writeConcern.concern, j = writeConcern.journal, wtimeout = wtimeout})

    local token = OPERATION_TOKEN
    OPERATION_TOKEN = OPERATION_TOKEN + 1
    LOG_DEBUG("MongoCollection:delete_, request",  {token=token, collection=self.name, cmd=pretty_show(cmd)})
    local success, r = pcall(self.database.runCommand, self.database, table.unpack(cmd))
    LOG_DEBUG("MongoCollection:delete_, response", {token=token, collection=self.name, success=success, r=r})
    if not success then
        return false, r
    else
        if r.ok == 1 and not r.writeErrors and not r.writeConcernError then
            return true, r.n
        else
            if r.errmsg then
                return false, r.errmsg
            elseif r.writeErrors then
                local errs = {}
                for _, err in ipairs(r.writeErrors) do
                    table.insert(errs, err.errmsg)
                end
                return false, table.concat(errs, '\n')
            else
                return false, r.writeConcernError.errmsg
            end
        end
    end
end

--[[
options:
    [use when you know what it is]:
        wtimeout
            >0
            default:30000
        writeConcern
            WRITE_CONCERN_PRIMARY|WRITE_CONCERN_MAJORITY
            default:WRITE_CONCERN_MAJORITY
--]]
function MongoCollection:delete_one(query, options)
    assert(not table.empty(query), "delete_one must take a `query` parameter in not-empty table")
    options = table.empty(options) and DEFAULT or options
    return delete_(self, {mongo.gen_delete_entry(query, 1)}, options)
end

--[[
options:
    [use when you know what it is]:
        ordered
            boolean
            default:true
        wtimeout
            >0
            default:30000
        writeConcern
            WRITE_CONCERN_PRIMARY|WRITE_CONCERN_MAJORITY
            default:WRITE_CONCERN_MAJORITY
--]]
function MongoCollection:delete_all(query, options)
    assert(not table.empty(query), "delete_all must take a `query` parameter in not-empty table")
    options = table.empty(options) and DEFAULT or options
    return delete_(self, {mongo.gen_delete_entry(query, 0)}, options)
end

--[[
options:
    [use when you know what it is]:
        ordered
            boolean
            default:true
        wtimeout
            >0
            default:30000
        writeConcern
            WRITE_CONCERN_PRIMARY|WRITE_CONCERN_MAJORITY
            default:WRITE_CONCERN_MAJORITY
--]]
function MongoCollection:delete_bulk(deleteEntries, options)
    assert(not table.empty(deleteEntries), "delete_bulk must take a `deleteEntries` parameter in not-empty table")
    for _, deleteEntry in ipairs(deleteEntries) do
        assert(getmetatable(deleteEntry) == DELETE_ENTRY_MT_READONLY, "delete_bulk must take a `deleteEntries` parameter which contains elements with DELETE_ENTRY_MT")
    end
    options = table.empty(options) and DEFAULT or options
    return delete_(self, deleteEntries, options)
end


--[[
{
    createIndexes: <collection>,
    indexes: [
        {
            key: {
                <key-value_pair>,
                <key-value_pair>,
                ...
            },
            name: <index_name>,
            <option1>,
            <option2>,
            ...
        },
        { ... },
        { ... }
    ],
    writeConcern: { <write concern> }
}

createIndexes(string)
    The collection for which to create indexes.
indexes(array)
    Specifies the indexes to create. Each document in the array specifies a separate index.
writeConcern(document)Optional
    A document expressing the write concern. Omit to use the default write concern.
    New in version 3.4.

key(document)
    Specifies the index’s fields.
    For each field, specify a key-value pair in which the key is the name of the field to index and the value is either the index direction or index type.
    If specifying direction, specify 1 for ascending or -1 for descending.
name(string)
    A name that uniquely identifies the index.
background(boolean)Optional
    Builds the index in the background so the operation does not block other database activities. Specify true to build in the background. The default value is false.
unique(boolean)Optional
    Creates a unique index so that the collection will not accept insertion or update of documents where the index key value matches an existing value in the index.
    Specify true to create a unique index.
    The default value is false.
    The option is unavailable for hashed indexes.
partialFilterExpression(document)Optional
    If specified, the index only references documents that match the filter expression. See Partial Indexes for more information.
    A filter expression can include:
        equality expressions (i.e. field: value or using the $eq operator),
        $exists: true expression,
        $gt, $gte, $lt, $lte expressions,
        $type expressions,
        $and operator at the top-level only
    You can specify a partialFilterExpression option for all MongoDB index types.
    New in version 3.2.
sparse(boolean)Optional
    If true, the index only references documents with the specified field.
    These indexes use less space but behave differently in some situations (particularly sorts).
    The default value is false.
    See Sparse Indexes for more information.
    Changed in version 3.2: Starting in MongoDB 3.2, MongoDB provides the option to create partial indexes.
    Partial indexes offer a superset of the functionality of sparse indexes.
    If you are using MongoDB 3.2 or later, partial indexes should be preferred over sparse indexes.
    Changed in version 2.6: 2dsphere indexes are sparse by default and ignore this option.
    For a compound index that includes 2dsphere index key(s) along with keys of other types, only the 2dsphere index fields determine whether the index references a document.
    2d, geoHaystack, and text indexes behave similarly to the 2dsphere indexes.
expireAfterSeconds(integer)Optional
    Specifies a value, in seconds, as a TTL to control how long MongoDB retains documents in this collection.
    See Expire Data from Collections by Setting TTL for more information on this functionality.
    This applies only to TTL indexes.
storageEngine(document)Optional
    Allows users to configure the storage engine on a per-index basis when creating an index.
    The storageEngine option should take the following form:
        storageEngine: { <storage-engine-name>: <options> }
    Storage engine configuration options specified when creating indexes are validated and logged to the oplog during replication to support replica sets with members that use different storage engines.
    New in version 3.0.
weights(document)Optional
    For text indexes, a document that contains field and weight pairs.
    The weight is an integer ranging from 1 to 99,999 and denotes the significance of the field relative to the other indexed fields in terms of the score.
    You can specify weights for some or all the indexed fields.
    See Control Search Results with Weights to adjust the scores.
    The default value is 1.
default_language(string)Optional
    For text indexes, the language that determines the list of stop words and the rules for the stemmer and tokenizer.
    See Text Search Languages for the available languages and Specify a Language for Text Index for more information and examples.
    The default value is english.
language_override(string)Optional
    For text indexes, the name of the field, in the collection’s documents, that contains the override language for the document.
    The default value is language.
    See Use any Field to Specify the Language for a Document for an example.
textIndexVersion(integer)Optional
    The text index version number. Users can use this option to override the default version number.
    For available versions, see Versions.
    New in version 2.6.
2dsphereIndexVersion(integer)Optional
    The 2dsphere index version number. Users can use this option to override the default version number.
    For the available versions, see Versions.
    New in version 2.6.
bits(integer)Optional.
    For 2d indexes, the number of precision of the stored geohash value of the location data.
    The bits value ranges from 1 to 32 inclusive. The default value is 26.
min(number)Optional
    For 2d indexes, the lower inclusive boundary for the longitude and latitude values.
    The default value is -180.0.
max(number)Optional
    For 2d indexes, the upper inclusive boundary for the longitude and latitude values.
    The default value is 180.0.
bucketSize(number)
    For geoHaystack indexes, specify the number of units within which to group the location values;
    i.e. group in the same bucket those location values that are within the specified number of units to each other.
    The value must be greater than 0.
collation(document)Optional
    Specifies the collation for the index.
    Collation allows users to specify language-specific rules for string comparison, such as rules for lettercase and accent marks.
    If you have specified a collation at the collection level, then:
        If you do not specify a collation when creating the index, MongoDB creates the index with the collection’s default collation.
        If you do specify a collation when creating the index, MongoDB creates the index with the specified collation.
    The collation option has the following syntax:
        collation: {
           locale: <string>,
           caseLevel: <boolean>,
           caseFirst: <string>,
           strength: <int>,
           numericOrdering: <boolean>,
           alternate: <string>,
           maxVariable: <string>,
           backwards: <boolean>
        }
    When specifying collation, the locale field is mandatory; all other collation fields are optional.
    For descriptions of the fields, see Collation Document.
    New in version 3.4.
--]]
local function create_index_(self, indexEntries, options)
    local cmd = {"createIndexes",self.name, "indexes",indexEntries}

    local wtimeout = options.wtimeout
    if math.type(wtimeout) ~= "integer" or wtimeout <= 0 then
        wtimeout = 30000
    end
    local writeConcern = WRITE_CONCERN_MAPPING[options.writeConcern]
    if not writeConcern then
        writeConcern = WRITE_CONCERN_MAJORITY
    end
    table.insert(cmd, "writeConcern")
    table.insert(cmd, {w = writeConcern.concern, j = writeConcern.journal, wtimeout = wtimeout})

    local token = OPERATION_TOKEN
    OPERATION_TOKEN = OPERATION_TOKEN + 1
    LOG_DEBUG("MongoCollection:create_index_, request",  {token=token, collection=self.name, cmd=pretty_show(cmd)})
    local success, r = pcall(self.database.runCommand, self.database, table.unpack(cmd))
    LOG_DEBUG("MongoCollection:create_index_, response", {token=token, collection=self.name, success=success, r=r})
    if not success then
        return false, r
    else
        if r.ok == 1 and not r.writeErrors and not r.writeConcernError then
            return true
        else
            if r.errmsg then
                return false, r.errmsg
            elseif r.writeErrors then
                local errs = {}
                for _, err in ipairs(r.writeErrors) do
                    table.insert(errs, err.errmsg)
                end
                return false, table.concat(errs, '\n')
            else
                return false, r.writeConcernError.errmsg
            end
        end
    end
end

--[[
options:
    [use when in development]:
        unique
            boolean
            default:false
        partialFilterExpression
            table
            default:nil
        background
            boolean
            default:false
    [use when you know what it is]:
        wtimeout
            >0
            default:30000
        writeConcern
            WRITE_CONCERN_PRIMARY|WRITE_CONCERN_MAJORITY
            default:WRITE_CONCERN_MAJORITY
--]]
function MongoCollection:create_index_one(key, name, options)
    assert(not table.empty(key), "creat_index_one must take a `key` parameter in not-empty table")
    assert(type(name) == "string" and #name>0, "creat_index_one must take a `name` parameter in not-empty string")
    options = table.empty(options) and DEFAULT or options
    return create_index_(self, {mongo.gen_index_entry(key, name, options)}, options)
end

--[[
options:
    [use when you know what it is]:
        wtimeout
            >0
            default:30000
        writeConcern
            WRITE_CONCERN_PRIMARY|WRITE_CONCERN_MAJORITY
            default:WRITE_CONCERN_MAJORITY
--]]
function MongoCollection:create_index_bulk(indexEntries, options)
    assert(not table.empty(indexEntries), "create_index_bulk must take a `indexEntries` parameter in not-empty table")
    for _, indexEntry in ipairs(indexEntries) do
        assert(getmetatable(indexEntry) == INDEX_ENTRY_MT_READONLY, "create_index_bulk must take a `indexEntries` parameter which contains elements with INDEX_ENTRY_MT")
    end
    options = table.empty(options) and DEFAULT or options
    return create_index_(self, indexEntries, options)
end


--[[
{
    dropIndexes: <string>,
    index: <string|document>,
    writeConcern: <document>
}

dropIndexes
    The name of the collection whose indexes to drop.
index
    The name or the specification document of the index to drop.
    To drop all non-_id indexes from the collection, specify "*".
    To drop a text index, specify the index name.
writeConcern(Optional)
    A document expressing the write concern of the drop command. Omit to use the default write concern.
--]]
local function drop_index_(self, index, options)
    local cmd = {"dropIndexes",self.name, "index",index}

    local wtimeout = options.wtimeout
    if math.type(wtimeout) ~= "integer" or wtimeout <= 0 then
        wtimeout = 30000
    end
    local writeConcern = WRITE_CONCERN_MAPPING[options.writeConcern]
    if not writeConcern then
        writeConcern = WRITE_CONCERN_MAJORITY
    end
    table.insert(cmd, "writeConcern")
    table.insert(cmd, {w = writeConcern.concern, j = writeConcern.journal, wtimeout = wtimeout})

    local token = OPERATION_TOKEN
    OPERATION_TOKEN = OPERATION_TOKEN + 1
    LOG_DEBUG("MongoCollection:drop_index_, request",  {token=token, collection=self.name, cmd=pretty_show(cmd)})
    local success, r = pcall(self.database.runCommand, self.database, table.unpack(cmd))
    LOG_DEBUG("MongoCollection:drop_index_, response", {token=token, collection=self.name, success=success, r=r})
    if not success then
        return false, r
    else
        if r.ok == 1 and not r.writeErrors and not r.writeConcernError then
            return true
        else
            if r.errmsg then
                return false, r.errmsg
            elseif r.writeErrors then
                local errs = {}
                for _, err in ipairs(r.writeErrors) do
                    table.insert(errs, err.errmsg)
                end
                return false, table.concat(errs, '\n')
            else
                return false, r.writeConcernError.errmsg
            end
        end
    end
end

--[[
options:
    [use when you know what it is]:
        wtimeout
            >0
            default:30000
        writeConcern
            WRITE_CONCERN_PRIMARY|WRITE_CONCERN_MAJORITY
            default:WRITE_CONCERN_MAJORITY
--]]
function MongoCollection:drop_index_one(index, options)
    assert((type(index) == "string" and #index>0) or not table.empty(index), "drop_index_one must take a `index` parameter in not-empty string or table")
    options = table.empty(options) and DEFAULT or options
    return drop_index_(self, index, options)
end

--[[
options:
    [use when you know what it is]:
        wtimeout
            >0
            default:30000
        writeConcern
            WRITE_CONCERN_PRIMARY|WRITE_CONCERN_MAJORITY
            default:WRITE_CONCERN_MAJORITY
--]]
function MongoCollection:drop_index_all(options)
    options = table.empty(options) and DEFAULT or options
    return drop_index_(self, "*", options)
end


--[[
{
    drop: <collection_name>,
    writeConcern: <document>
}

drop
    The name of the collection to drop.
writeConcern(Optional)
    A document expressing the write concern of the drop command. Omit to use the default write concern.
    When issued on a sharded cluster, mongos converts the write concern of the drop command and its helper db.collection.drop() to "majority".
--]]
local function drop_(self, options)
    local cmd = {"drop",self.name}

    local wtimeout = options.wtimeout
    if math.type(wtimeout) ~= "integer" or wtimeout <= 0 then
        wtimeout = 30000
    end
    local writeConcern = WRITE_CONCERN_MAJORITY
    table.insert(cmd, "writeConcern")
    table.insert(cmd, {w = writeConcern.concern, j = writeConcern.journal, wtimeout = wtimeout})

    local token = OPERATION_TOKEN
    OPERATION_TOKEN = OPERATION_TOKEN + 1
    LOG_DEBUG("MongoCollection:drop_, request",  {token=token, collection=self.name, cmd=pretty_show(cmd)})
    local success, r = pcall(self.database.runCommand, self.database, table.unpack(cmd))
    LOG_DEBUG("MongoCollection:drop_, response", {token=token, collection=self.name, success=success, r=r})
    if not success then
        return false, r
    else
        if r.ok == 1 and not r.writeErrors and not r.writeConcernError then
            return true
        else
            if r.errmsg then
                return false, r.errmsg
            elseif r.writeErrors then
                local errs = {}
                for _, err in ipairs(r.writeErrors) do
                    table.insert(errs, err.errmsg)
                end
                return false, table.concat(errs, '\n')
            else
                return false, r.writeConcernError.errmsg
            end
        end
    end
end

--[[
options:
    [use when you know what it is]:
        wtimeout
            >0
            default:30000
--]]
function MongoCollection:drop(options)
    options = table.empty(options) and DEFAULT or options
    return drop_(self, options)
end


--[[
{
  aggregate: "<collection>" || 1,
  pipeline: [ <stage>, <...> ],
  explain: <boolean>,
  allowDiskUse: <boolean>,
  cursor: <document>,
  maxTimeMS: <int>,
  bypassDocumentValidation: <boolean>,
  readConcern: <document>,
  collation: <document>,
  hint: <string or document>,
  comment: <string>,
  writeConcern: <document>
}

aggregate(string)
    he name of the collection or view that acts as the input for the aggregation pipeline.
    Use 1 for collection agnostic commands.
pipeline(array)
    An array of aggregation pipeline stages that process and transform the document stream as part of the aggregation pipeline.
allowDiskUse(boolean)Optional.
    Enables writing to temporary files.
    When set to true, aggregation stages can write data to the _tmp subdirectory in the dbPath directory.
cursor(document)
    Unless you include the explain option, you must specify the cursor option.
    To indicate a cursor with the default batch size, specify cursor: {}.
    To indicate a cursor with a non-default batch size, use cursor: { batchSize: <num> }.
maxTimeMS(non-negative integer)Optional.
    Specifies a time limit in milliseconds for processing operations on a cursor.
    If you do not specify a value for maxTimeMS, operations will not time out.
    A value of 0 explicitly specifies the default unbounded behavior.
readConcern(document)Optional.
    Specifies the read concern.
    The readConcern option has the following syntax:
    Changed in version 3.6.
    readConcern: { level: <value> }
    Possible read concern levels are:
        "local". This is the default read concern level.
        "available". This is the default for reads against secondaries when Read Operations and afterClusterTime and “level” are unspecified. The query returns the instance’s most recent data.
        "majority". Available for replica sets that use WiredTiger storage engine.
        "linearizable". Available for read operations on the primary only.
    For more formation on the read concern levels, see Read Concern Levels.
    For "local" (default) or "majority" read concern level, you can specify the afterClusterTime option to have the read operation return data that meets the level requirement and the specified after cluster time requirement.
    For more information, see Read Operations and afterClusterTime.
    The getMore command uses the readConcern level specified in the originating find command.
collation(document)Optional.
    Specifies the collation to use for the operation.
    Collation allows users to specify language-specific rules for string comparison, such as rules for lettercase and accent marks.
    The collation option has the following syntax:
        collation: {
           locale: <string>,
           caseLevel: <boolean>,
           caseFirst: <string>,
           strength: <int>,
           numericOrdering: <boolean>,
           alternate: <string>,
           maxVariable: <string>,
           backwards: <boolean>
        }
    When specifying collation, the locale field is mandatory; all other collation fields are optional.
    For descriptions of the fields, see Collation Document.
    If the collation is unspecified but the collection has a default collation (see db.createCollection()), the operation uses the collation specified for the collection.
    If no collation is specified for the collection or for the operations, MongoDB uses the simple binary comparison used in prior versions for string comparisons.
    You cannot specify multiple collations for an operation.
    For example, you cannot specify different collations per field, or if performing a find with a sort, you cannot use one collation for the find and another for the sort.
    New in version 3.4.
hint(string or document)Optional.
    The index to use for the aggregation.
    The index is on the initial collection/view against which the aggregation is run.
    Specify the index either by the index name or by the index specification document.
--]]
local function aggregate_(self, pipeline, options)
    local cmd = {"aggregate",self.name, "pipeline",pipeline}

    local hint = options.hint
    if type(hint) == "string" or not table.empty(hint) then
        table.insert(cmd, "hint")
        table.insert(cmd, hint)
    end

    table.insert(cmd, "cursor")
    local batchSize = options.batchSize
    if math.type(batchSize) == "integer" and batchSize >= 0 then
        table.insert(cmd, {batchSize = batchSize})
    else
        table.insert(cmd, {})
    end

    local allowDiskUse = options.allowDiskUse
    if allowDiskUse == nil then
        allowDiskUse = true
    end
    table.insert(cmd, "allowDiskUse")
    table.insert(cmd, not not allowDiskUse)

    local maxTimeMS = options.maxTimeMS
    if math.type(maxTimeMS) ~= "integer" or maxTimeMS <= 0 then
        maxTimeMS = 30000
    end
    table.insert(cmd, "maxTimeMS")
    table.insert(cmd, maxTimeMS)

    local readConcern = READ_CONCERN_MAPPING[options.readConcern]
    if not readConcern then
        readConcern = READ_CONCERN_MAJORITY
    end
    table.insert(cmd, "readConcern")
    table.insert(cmd, {level = readConcern.concern})

    local readPreference = READ_PREFERENCE_MAPPING[options.readPreference]
    if not readPreference then
        readPreference = READ_PREFERENCE_SECONDARYPREFERRED
    end
    if readConcern == READ_CONCERN_LINEARIZABLE then
        readPreference = READ_PREFERENCE_PRIMARY
    end
    table.insert(cmd, "$readPreference")
    table.insert(cmd, {mode = readPreference.preference})

    local token = OPERATION_TOKEN
    OPERATION_TOKEN = OPERATION_TOKEN + 1
    LOG_DEBUG("MongoCollection:aggregate_, request",  {token=token, collection=self.name, cmd=pretty_show(cmd)})
    local success, r = pcall(self.database.runCommand, self.database, table.unpack(cmd))
    LOG_DEBUG("MongoCollection:aggregate_, response", {token=token, collection=self.name, success=success, r=r})
    if not success then
        return false, r
    else
        if r.ok == 1 and not r.readErrors and not r.readConcernError then
            return true, r.cursor.firstBatch, r.cursor.id
        else
            if r.errmsg then
                return false, r.errmsg
            elseif r.readErrors then
                local errs = {}
                for _, err in ipairs(r.readErrors) do
                    table.insert(errs, err.errmsg)
                end
                return false, table.concat(errs, '\n')
            else
                return false, r.readConcernError.errmsg
            end
        end
    end
end

--[[
options:
    [use when you know what it is]:
        hint
            string|table
            xxx|{xxx:1, xxx:-1, xxx:1}
            default:nil
        batchSize
            >=0
            default:101
        allowDiskUse
            boolean
            default:true
        maxTimeMS
            >0
            default:30000
        readConcern
            READ_CONCERN_LOCAL|READ_CONCERN_MAJORITY
            default:READ_CONCERN_MAJORITY
        readPreference
            READ_PREFERENCE_PRIMARY|READ_PREFERENCE_SECONDARY|READ_PREFERENCE_PRIMARYPREFERRED|READ_PREFERENCE_SECONDARYPREFERRED|READ_PREFERENCE_NEAREST
            default:READ_PREFERENCE_SECONDARYPREFERRED
--]]
function MongoCollection:aggregate(pipeline, options)
    assert(not table.empty(pipeline), "aggregate must take a `pipeline` parameter in not-empty table")
    for _, stage in ipairs(pipeline) do
        assert(not table.empty(stage), "aggregate must take a `pipeline` parameter which contains not-empty table")
    end
    options = table.empty(options) and DEFAULT or options
    return aggregate_(self, pipeline, options)
end


return mongo
