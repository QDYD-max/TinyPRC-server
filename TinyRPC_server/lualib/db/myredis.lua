-- Support for Redis 5.x

local skynet = require "skynet"
local socketchannel = require "skynet.socketchannel"

local table = table
local math = math
local tonumber = tonumber
local string = string
local setmetatable = setmetatable
local ipairs = ipairs

local redis = {}


--region Request Protocol

local function make_string_cache(genFunc)
    return setmetatable({}, {
        __mode = "kv",
        __index = genFunc
    })
end

local arrayLenCache = make_string_cache(function(cache, len)
    local str = string.format("*%d\r\n", len)
    cache[len] = str
    return str
end)

local stringLenCache = make_string_cache(function(cache, len)
    local str = string.format("$%d\r\n", len)
    cache[len] = str
    return str
end)

local cmdCache = make_string_cache(function(cache, cmd)
    cmd = cmd:upper()
    local str = string.format("$%d\r\n%s\r\n", #cmd, cmd)
    cache[cmd] = str
    return str
end)

local function make_request(cmd, paramArray)
    local requestArray = {}
    table.insert(requestArray, arrayLenCache[1+paramArray.n])
    table.insert(requestArray, cmdCache[cmd])
    for _, param in ipairs(paramArray) do
        local bulkString = string.format("%s\r\n", param)
        table.insert(requestArray, stringLenCache[#bulkString-2])
        table.insert(requestArray, bulkString)
    end
    return requestArray
end

--endregion


--region Response Protocol

local parse_response

local function parse_simple_string(fd, data)
    return true, data
end

local function parse_error(fd, data)
    return false, data
end

local function parse_integer(fd, data)
    local integer = math.tointeger(data)
    if not integer then
        return false, "parse integer error"
    end
    return true, integer
end

local function parse_bulk_string(fd, data)
    local len = math.tointeger(data)
    if not len then
        return false, "parse bulk string length error"
    end

    if len < 0 then
        return true, nil
    end

    local success, bulkString = pcall(fd.read, fd, len+2)
    if not success then
        return false, bulkString
    end
    return true, bulkString:sub(1, len)
end

local function parse_array(fd, data)
    local len = math.tointeger(data)
    if not len then
        return false, "parse array length error"
    end

    if len < 0 then
        return true, nil
    end

    local array = {}
    for idx = 1, len do
        local success, value = parse_response(fd)
        if not success then
            return false, value
        end
        array[idx] = value
    end
    return true, array
end

local parseFuncs = {
    ["+"] = parse_simple_string,
    ["-"] = parse_error,
    [":"] = parse_integer,
    ["$"] = parse_bulk_string,
    ["*"] = parse_array
}

function parse_response(fd)
    local success, line = pcall(fd.readline, fd, "\r\n")
    if not success then
        return false, line
    end

    local parse_func = parseFuncs[line[1]]
    if not parse_func then
        return false, "parse RESP type error"
    end
    return parse_func(fd, line:sub(2))
end

--endregion


local COMMAND_PARSE     = {}
local TRANSACTION_PARSE = {}
local PUBSUB_PARSE      = {}
local RESPONSE_PARSE    = {}

local function _common_parse(cmd, ...)
    return make_request(cmd, table.pack(...))
end


--region Connection Command

--[[
------------------------------------------------------------------------------------------------------------------------
AUTH password
Available since 1.0.0.
Simple string reply
------------------------------------------------------------------------------------------------------------------------

Request for authentication in a password-protected Redis server.
Redis can be instructed to require a password before allowing clients to execute commands.
This is done using the requirepass directive in the configuration file.

If password matches the password in the configuration file, the server replies with the OK status code and starts accepting commands.
Otherwise, an error is returned and the clients needs to try a new password.
--]]
function COMMAND_PARSE.AUTH(password)
    return _common_parse("AUTH", password)
end
function RESPONSE_PARSE.AUTH(r)
    return true
end

--[[
------------------------------------------------------------------------------------------------------------------------
PING [message]
Available since 1.0.0.
Simple string reply
------------------------------------------------------------------------------------------------------------------------

Returns PONG if no argument is provided, otherwise return a copy of the argument as a bulk.
This command is often used to test if a connection is still alive, or to measure latency.
--]]
function COMMAND_PARSE.PING(message)
    if message then
        return _common_parse("PING", message)
    end
    return _common_parse("PING")
end

--[[
------------------------------------------------------------------------------------------------------------------------
SELECT index
Available since 1.0.0.
Simple string reply
------------------------------------------------------------------------------------------------------------------------

Select the Redis logical database having the specified zero-based numeric index. New connections always use the database 0.

Selectable Redis databases are a form of namespacing: all databases are still persisted in the same RDB / AOF file.
However different databases can have keys with the same name, and commands like FLUSHDB, SWAPDB or RANDOMKEY work on specific databases.

When using Redis Cluster, the SELECT command cannot be used, since Redis Cluster only supports database zero.
In the case of a Redis Cluster, having multiple databases would be useless and an unnecessary source of complexity.
Commands operating atomically on a single database would not be possible with the Redis Cluster design and goals.
--]]
function COMMAND_PARSE.SELECT(index)
    return _common_parse("SELECT", index)
end
function RESPONSE_PARSE.SELECT(r)
    return true
end

--endregion


--region Key Command

--[[
------------------------------------------------------------------------------------------------------------------------
DEL key [key ...]
Available since 1.0.0.
Time complexity: O(N) where N is the number of keys that will be removed.
When a key to remove holds a value other than a string, the individual complexity for this key is O(M) where M is the number of elements in the list, set, sorted set or hash.
Removing a single key that holds a string value is O(1).
Integer reply: The number of keys that were removed.
------------------------------------------------------------------------------------------------------------------------

Removes the specified keys. A key is ignored if it does not exist.
--]]
function COMMAND_PARSE.DEL(key1, ...)
    return _common_parse("DEL", key1, ...)
end

--endregion


--region String Command

--[[
------------------------------------------------------------------------------------------------------------------------
APPEND key value
Available since 2.0.0
Time complexity: O(1)
Integer reply: the length of the string after the append operation.
------------------------------------------------------------------------------------------------------------------------

If key already exists and is a string, this command appends the value at the end of the string.
If key does not exist it is created and set as an empty string, so APPEND will be similar to SET in this special case.
--]]
function COMMAND_PARSE.APPEND(key, value)
    return _common_parse("APPEND", key, value)
end

--[[
------------------------------------------------------------------------------------------------------------------------
BITCOUNT key [start end]
Available since 2.6.0
Time complexity: O(N)
Integer reply: The number of bits set to 1.
------------------------------------------------------------------------------------------------------------------------

Count the number of set bits (population counting) in a string.

By default all the bytes contained in the string are examined.
It is possible to specify the counting operation only in an interval passing the additional arguments start and end.

Like for the GETRANGE command start and end can contain negative values in order to index bytes starting from the end of the string,
where -1 is the last byte, -2 is the penultimate, and so forth.

Non-existent keys are treated as empty strings, so the command will return zero.
--]]
function COMMAND_PARSE.BITCOUNT(key, startByte, endByte)
    startByte = math.tointeger(startByte)
    endByte = math.tointeger(endByte)
    local params = { key }
    if startByte and endByte then
        table.insert(params, startByte)
        table.insert(params, endByte)
    end

    params.n = #params
    return make_request("BITCOUNT", params)
end

--[[
------------------------------------------------------------------------------------------------------------------------
BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL]
Available since 3.2.0
Time complexity: O(1) for each subcommand specified
Return value: The command returns an array with each entry being the corresponding result of the sub command given at the same position.
OVERFLOW subcommands don't count as generating a reply.
------------------------------------------------------------------------------------------------------------------------

The command treats a Redis string as a array of bits, and is capable of addressing specific integer fields of varying bit widths and arbitrary non (necessary) aligned offset.
In practical terms using this command you can set, for example, a signed 5 bits integer at bit offset 1234 to a specific value, retrieve a 31 bit unsigned integer from offset 4567.
Similarly the command handles increments and decrements of the specified integers, providing guaranteed and well specified overflow and underflow behavior that the user can configure.

BITFIELD is able to operate with multiple bit fields in the same command call.
It takes a list of operations to perform, and returns an array of replies, where each array matches the corresponding operation in the list of arguments.

Addressing with GET bits outside the current string length (including the case the key does not exist at all),
results in the operation to be performed like the missing part all consists of bits set to 0.

Addressing with SET or INCRBY bits outside the current string length will enlarge the string, zero-padding it, as needed,
for the minimal length needed, according to the most far bit touched.

GET <type> <offset> -- Returns the specified bit field.
SET <type> <offset> <value> -- Set the specified bit field and returns its old value.
INCRBY <type> <offset> <increment> -- Increments or decrements (if a negative increment is given) the specified bit field and returns the new value.
OVERFLOW [WRAP|SAT|FAIL] -- only changes the behavior of successive INCRBY subcommand calls by setting the overflow behavior

Where an integer type is expected, it can be composed by prefixing with i for signed integers and u for unsigned integers with the number of bits of our integer type.
So for example u8 is an unsigned integer of 8 bits and i16 is a signed integer of 16 bits.

There are two ways in order to specify offsets in the bitfield command.
If a number without any prefix is specified, it is used just as a zero based bit offset inside the string.
However if the offset is prefixed with a # character, the specified offset is multiplied by the integer type width.

WRAP: wrap around, both with signed and unsigned integers.
In the case of unsigned integers, wrapping is like performing the operation modulo the maximum value the integer can contain (the C standard behavior).
With signed integers instead wrapping means that overflows restart towards the most negative value and underflows towards the most positive ones,
so for example if an i8 integer is set to the value 127, incrementing it by 1 will yield -128.

SAT: uses saturation arithmetic, that is, on underflows the value is set to the minimum integer value, and on overflows to the maximum integer value.
For example incrementing an i8 integer starting from value 120 with an increment of 10, will result into the value 127, and further increments will always keep the value at 127.
The same happens on underflows, but towards the value is blocked at the most negative value.

FAIL: in this mode no operation is performed on overflows or underflows detected. The corresponding return value is set to NULL to signal the condition to the caller.

each OVERFLOW statement only affects the INCRBY commands that follow it in the list of subcommands, up to the next OVERFLOW statement.
By default, WRAP is used if not otherwise specified.
--]]
function COMMAND_PARSE.BITFIELD(key, operations)
    local params = { key }
    local curOverflow = "WRAP"
    for _, operation in ipairs(operations) do
        local op = operation.op
        if op == "GET" then
            table.insert(params, "GET")
            table.insert(params, operation.type)
            table.insert(params, operation.offset)
        elseif op == "SET" then
            table.insert(params, "SET")
            table.insert(params, operation.type)
            table.insert(params, operation.offset)
            table.insert(params, math.tointeger(operation.value))
        elseif op == "INCRBY" then
            local overflow = operation.overflow
            if overflow ~= "SAT" and overflow ~= "FAIL" then
                overflow = "WRAP"
            end
            if overflow ~= curOverflow then
                curOverflow = overflow
                table.insert(params, "OVERFLOW")
                table.insert(params, overflow)
            end
            table.insert(params, "INCRBY")
            table.insert(params, operation.type)
            table.insert(params, operation.offset)
            table.insert(params, math.tointeger(operation.value))
        end
    end

    params.n = #params
    return make_request("BITFIELD", params)
end

--[[
------------------------------------------------------------------------------------------------------------------------
BITOP operation destkey key [key ...]
Available since 2.6.0
Time complexity: O(N)
Integer reply: The size of the string stored in the destination key, that is equal to the size of the longest input string.
------------------------------------------------------------------------------------------------------------------------

Perform a bitwise operation between multiple keys (containing string values) and store the result in the destination key.

The BITOP command supports four bitwise operations: AND, OR, XOR and NOT, thus the valid forms to call the command are:
BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
BITOP OR destkey srckey1 srckey2 srckey3 ... srckeyN
BITOP XOR destkey srckey1 srckey2 srckey3 ... srckeyN
BITOP NOT destkey srckey

When an operation is performed between strings having different lengths,
all the strings shorter than the longest string in the set are treated as if they were zero-padded up to the length of the longest string.
The same holds true for non-existent keys, that are considered as a stream of zero bytes up to the length of the longest string.
--]]
function COMMAND_PARSE.BITOP(operation, destKey, srcKey1, ...)
    return _common_parse("BITOP", operation, destKey, srcKey1, ...)
end

--[[
------------------------------------------------------------------------------------------------------------------------
BITPOS key bit [start] [end]
Available since 2.8.7
Time complexity: O(N)
Integer reply: The command returns the position of the first bit set to 1 or 0 according to the request.
------------------------------------------------------------------------------------------------------------------------

Return the position of the first bit set to 1 or 0 in a string.

The position is returned, thinking of the string as an array of bits from left to right,
where the first byte's most significant bit is at position 0, the second byte's most significant bit is at position 8, and so forth.

By default, all the bytes contained in the string are examined.
It is possible to look for bits only in a specified interval passing the additional arguments start and end (it is possible to just pass start, the operation will assume that the end is the last byte of the string. However there are semantic differences as explained later).
The range is interpreted as a range of bytes and not a range of bits, so start=0 and end=2 means to look at the first three bytes.

Note that bit positions are returned always as absolute values starting from bit zero even when start and end are used to specify a range.

Like for the GETRANGE command start and end can contain negative values in order to index bytes starting from the end of the string,
where -1 is the last byte, -2 is the penultimate, and so forth.

Non-existent keys are treated as empty strings.
--]]
function COMMAND_PARSE.BITPOS(key, bit, startByte, endByte)
    bit = math.tointeger(bit)
    if bit ~= 1 then
        bit = 0
    end
    startByte = math.tointeger(startByte)
    endByte = math.tointeger(endByte)
    local params = { key, bit }
    if startByte then
        table.insert(params, startByte)
        if endByte then
            table.insert(params, endByte)
        end
    end

    params.n = #params
    return make_request("BITPOS", params)
end

--[[
------------------------------------------------------------------------------------------------------------------------
DECR key
Available since 1.0.0
Time complexity: O(1)
Integer reply: the value of key after the decrement
------------------------------------------------------------------------------------------------------------------------

Decrements the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
An error is returned if the key contains a value of the wrong type or contains a string that can not be represented as integer.
This operation is limited to 64 bit signed integers.
--]]
function COMMAND_PARSE.DECR(key)
    return _common_parse("DECR", key)
end

--[[
------------------------------------------------------------------------------------------------------------------------
DECRBY key decrement
Available since 1.0.0
Time complexity: O(1)
Integer reply: the value of key after the decrement
------------------------------------------------------------------------------------------------------------------------

Decrements the number stored at key by decrement. If the key does not exist, it is set to 0 before performing the operation.
An error is returned if the key contains a value of the wrong type or contains a string that can not be represented as integer.
This operation is limited to 64 bit signed integers.
--]]
function COMMAND_PARSE.DECRBY(key, decrement)
    decrement = math.tointeger(decrement)
    return _common_parse("DECRBY", key, decrement)
end

--[[
------------------------------------------------------------------------------------------------------------------------
GET key
Available since 1.0.0
Time complexity: O(1)
Bulk string reply: the value of key, or nil when key does not exist.
------------------------------------------------------------------------------------------------------------------------

Get the value of key. If the key does not exist the special value nil is returned.
An error is returned if the value stored at key is not a string, because GET only handles string values.
--]]
function COMMAND_PARSE.GET(key)
    return _common_parse("GET", key)
end

--[[
------------------------------------------------------------------------------------------------------------------------
GETBIT key offset
Available since 2.2.0
Time complexity: O(1)
Integer reply: the bit value stored at offset.
------------------------------------------------------------------------------------------------------------------------

Returns the bit value at offset in the string value stored at key.

When offset is beyond the string length, the string is assumed to be a contiguous space with 0 bits.
When key does not exist it is assumed to be an empty string, so offset is always out of range and the value is also assumed to be a contiguous space with 0 bits.
--]]
function COMMAND_PARSE.GETBIT(key, offset)
    offset = math.tointeger(offset)
    return _common_parse("GETBIT", key, offset)
end

--[[
------------------------------------------------------------------------------------------------------------------------
GETRANGE key start end
Available since 2.4.0
Time complexity: O(N) where N is the length of the returned string.
Bulk string reply
------------------------------------------------------------------------------------------------------------------------

Returns the substring of the string value stored at key, determined by the offsets start and end (both are inclusive).
Negative offsets can be used in order to provide an offset starting from the end of the string. So -1 means the last character, -2 the penultimate and so forth.

The function handles out of range requests by limiting the resulting range to the actual length of the string.
--]]
function COMMAND_PARSE.GETRANGE(key, startByte, endByte)
    startByte = math.tointeger(startByte) or 0
    endByte = math.tointeger(endByte) or -1
    return _common_parse("GETRANGE", key, startByte, endByte)
end

--[[
------------------------------------------------------------------------------------------------------------------------
GETSET key value
Available since 1.0.0
Time complexity: O(1)
Bulk string reply: the old value stored at key, or nil when key did not exist.
------------------------------------------------------------------------------------------------------------------------

Atomically sets key to value and returns the old value stored at key.
Returns an error when key exists but does not hold a string value.
--]]
function COMMAND_PARSE.GETSET(key, value)
    return _common_parse("GETSET", key, value)
end

--[[
------------------------------------------------------------------------------------------------------------------------
INCR key
Available since 1.0.0
Time complexity: O(1)
Integer reply: the value of key after the increment
------------------------------------------------------------------------------------------------------------------------

Increments the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
An error is returned if the key contains a value of the wrong type or contains a string that can not be represented as integer.
This operation is limited to 64 bit signed integers.
--]]
function COMMAND_PARSE.INCR(key)
    return _common_parse("INCR", key)
end

--[[
------------------------------------------------------------------------------------------------------------------------
INCRBY key increment
Available since 1.0.0
Time complexity: O(1)
Integer reply: the value of key after the increment
------------------------------------------------------------------------------------------------------------------------

Increments the number stored at key by increment. If the key does not exist, it is set to 0 before performing the operation.
An error is returned if the key contains a value of the wrong type or contains a string that can not be represented as integer.
This operation is limited to 64 bit signed integers.
--]]
function COMMAND_PARSE.INCRBY(key, increment)
    increment = math.tointeger(increment)
    return _common_parse("INCRBY", key, increment)
end

--[[
------------------------------------------------------------------------------------------------------------------------
INCRBYFLOAT key increment
Available since 2.6.0
Time complexity: O(1)
Bulk string reply: the value of key after the increment.
------------------------------------------------------------------------------------------------------------------------

Increment the string representing a floating point number stored at key by the specified increment.
By using a negative increment value, the result is that the value stored at the key is decremented (by the obvious properties of addition).
If the key does not exist, it is set to 0 before performing the operation.
An error is returned if one of the following conditions occur:
  - The key contains a value of the wrong type (not a string).
  - The current key content or the specified increment are not parsable as a double precision floating point number.

If the command is successful the new incremented value is stored as the new value of the key (replacing the old one), and returned to the caller as a string.
--]]
function COMMAND_PARSE.INCRBYFLOAT(key, increment)
    return _common_parse("INCRBYFLOAT", key, increment)
end
function RESPONSE_PARSE.INCRBYFLOAT(r)
    return tonumber(r)
end

--[[
------------------------------------------------------------------------------------------------------------------------
MGET key [key ...]
Available since 1.0.0
Time complexity: O(N) where N is the number of keys to retrieve.
Array reply: list of values at the specified keys.
------------------------------------------------------------------------------------------------------------------------

Returns the values of all specified keys.
For every key that does not hold a string value or does not exist, the special value nil is returned. Because of this, the operation never fails.
--]]
function COMMAND_PARSE.MGET(key1, ...)
    return _common_parse("MGET", key1, ...)
end

--[[
------------------------------------------------------------------------------------------------------------------------
MSET key value [key value ...]
Available since 1.0.1
Time complexity: O(N) where N is the number of keys to set.
Simple string reply: always OK since MSET can't fail.
------------------------------------------------------------------------------------------------------------------------

Sets the given keys to their respective values. MSET replaces existing values with new values, just as regular SET. See MSETNX if you don't want to overwrite existing values.

MSET is atomic, so all given keys are set at once. It is not possible for clients to see that some of the keys were updated while others are unchanged.
--]]
function COMMAND_PARSE.MSET(kvPairs)
    local params = {}
    for k, v in pairs(kvPairs) do
        table.insert(params, k)
        table.insert(params, v)
    end

    params.n = #params
    return make_request("MSET", params)
end
function RESPONSE_PARSE.MSET(r)
    return true
end

--[[
------------------------------------------------------------------------------------------------------------------------
MSETNX key value [key value ...]
Available since 1.0.1
Time complexity: O(N) where N is the number of keys to set.
Integer reply, specifically:
  - 1 if the all the keys were set.
  - 0 if no key was set (at least one key already existed).
------------------------------------------------------------------------------------------------------------------------

Sets the given keys to their respective values. MSETNX will not perform any operation at all even if just a single key already exists.

Because of this semantic MSETNX can be used in order to set different keys representing different fields of an unique logic object in a way that ensures that either all the fields or none at all are set.

MSETNX is atomic, so all given keys are set at once. It is not possible for clients to see that some of the keys were updated while others are unchanged.
--]]
function COMMAND_PARSE.MSETNX(kvPairs)
    local params = {}
    for k, v in pairs(kvPairs) do
        table.insert(params, k)
        table.insert(params, v)
    end

    params.n = #params
    return make_request("MSETNX", params)
end
function RESPONSE_PARSE.MSETNX(r)
    return r == 1
end

--[[
------------------------------------------------------------------------------------------------------------------------
PSETEX key milliseconds value
Available since 2.6.0
Time complexity: O(1)
Simple string reply
------------------------------------------------------------------------------------------------------------------------

PSETEX works exactly like SETEX with the sole difference that the expire time is specified in milliseconds instead of seconds.
--]]
function COMMAND_PARSE.PSETEX(key, value, milliseconds)
    milliseconds = math.tointeger(milliseconds)
    return _common_parse("PSETEX", key, milliseconds, value)
end
function RESPONSE_PARSE.PSETEX(r)
    return true
end

--[[
------------------------------------------------------------------------------------------------------------------------
SET key value [EX seconds|PX milliseconds] [NX|XX] [KEEPTTL]
Available since 1.0.0
  - >= 2.6.12: Added the EX, PX, NX and XX options.
  - >= 6.0: Added the KEEPTTL option.
Time complexity: O(1)
Simple string reply: OK if SET was executed correctly.
Null reply: a Null Bulk Reply is returned if the SET operation was not performed because the user specified the NX or XX option but the condition was not met.
------------------------------------------------------------------------------------------------------------------------

Set key to hold the string value. If key already holds a value, it is overwritten, regardless of its type.
Any previous time to live associated with the key is discarded on successful SET operation.

EX seconds -- Set the specified expire time, in seconds.
PX milliseconds -- Set the specified expire time, in milliseconds.
NX -- Only set the key if it does not already exist.
XX -- Only set the key if it already exist.
KEEPTTL -- Retain the time to live associated with the key.
--]]
function COMMAND_PARSE.SET(key, value, expireFlag, expireTime, existsFlag)
    local params = { key, value }
    if expireFlag == "EX" or expireFlag == "PX" then
        expireTime = math.tointeger(expireTime)
        table.insert(params, expireFlag)
        table.insert(params, expireTime)
    end
    if existsFlag == "NX" or existsFlag == "XX" then
        table.insert(params, existsFlag)
    end

    params.n = #params
    return make_request("SET", params)
end
function RESPONSE_PARSE.SET(r)
    return not not r
end

--[[
------------------------------------------------------------------------------------------------------------------------
SETBIT key offset value
Available since 2.2.0.
Time complexity: O(1)
Integer reply: the original bit value stored at offset.
------------------------------------------------------------------------------------------------------------------------

Sets or clears the bit at offset in the string value stored at key.

The bit is either set or cleared depending on value, which can be either 0 or 1.

When key does not exist, a new string value is created. The string is grown to make sure it can hold a bit at offset.
The offset argument is required to be greater than or equal to 0, and smaller than 2^32 (this limits bitmaps to 512MB).
When the string at key is grown, added bits are set to 0.
--]]
function COMMAND_PARSE.SETBIT(key, offset, bit)
    offset = math.tointeger(offset)
    bit = math.tointeger(bit)
    if bit ~= 1 then
        bit = 0
    end
    return _common_parse("SETBIT", key, offset, bit)
end

--[[
------------------------------------------------------------------------------------------------------------------------
SETEX key seconds value
Available since 2.0.0
Time complexity: O(1)
Simple string reply
------------------------------------------------------------------------------------------------------------------------

Set key to hold the string value and set key to timeout after a given number of seconds.
--]]
function COMMAND_PARSE.SETEX(key, value, seconds)
    seconds = math.tointeger(seconds)
    return _common_parse("SETEX", key, seconds, value)
end
function RESPONSE_PARSE.SETEX(r)
    return true
end

--[[
------------------------------------------------------------------------------------------------------------------------
SETNX key value
Available since 1.0.0
Time complexity: O(1)
Integer reply, specifically:
  - 1 if the key was set
  - 0 if the key was not set
------------------------------------------------------------------------------------------------------------------------

Set key to hold string value if key does not exist. In that case, it is equal to SET.
When key already holds a value, no operation is performed. SETNX is short for "SET if Not eXists".
--]]
function COMMAND_PARSE.SETNX(key, value)
    return _common_parse("SETNX", key, value)
end
function RESPONSE_PARSE.SETNX(r)
    return r == 1
end

--[[
------------------------------------------------------------------------------------------------------------------------
SETRANGE key offset value
Available since 2.2.0
Time complexity: O(1), not counting the time taken to copy the new string in place.
Integer reply: the length of the string after it was modified by the command.
------------------------------------------------------------------------------------------------------------------------

Overwrites part of the string stored at key, starting at the specified offset, for the entire length of value.
If the offset is larger than the current length of the string at key, the string is padded with zero-bytes to make offset fit.
Non-existing keys are considered as empty strings, so this command will make sure it holds a string large enough to be able to set value at offset.
Note that the maximum offset that you can set is 2^29 -1
--]]
function COMMAND_PARSE.SETRANGE(key, offset, value)
    offset = math.tointeger(offset)
    return _common_parse("SETRANGE", key, offset, value)
end

--[[
------------------------------------------------------------------------------------------------------------------------
STRLEN key
Available since 2.2.0
Time complexity: O(1)
Integer reply: the length of the string at key, or 0 when key does not exist.
------------------------------------------------------------------------------------------------------------------------

Returns the length of the string value stored at key. An error is returned when key holds a non-string value.
--]]
function COMMAND_PARSE.STRLEN(key)
    return _common_parse("STRLEN", key)
end

--endregion


--region Hash Command

--[[
------------------------------------------------------------------------------------------------------------------------
HDEL key field [field ...]
Available since 2.0.0
  - >= 2.4: Accepts multiple field arguments. Redis versions older than 2.4 can only remove a field per call.
Time complexity: O(N) where N is the number of fields to be removed.
Integer reply: the number of fields that were removed from the hash, not including specified but non existing fields.
------------------------------------------------------------------------------------------------------------------------

Removes the specified fields from the hash stored at key. Specified fields that do not exist within this hash are ignored.
If key does not exist, it is treated as an empty hash and this command returns 0.
--]]
function COMMAND_PARSE.HDEL(key, field1, ...)
    return _common_parse("HDEL", key, field1, ...)
end

--[[
------------------------------------------------------------------------------------------------------------------------
HEXISTS key field
Available since 2.0.0
Time complexity: O(1)
Integer reply, specifically:
  - 1 if the hash contains field.
  - 0 if the hash does not contain field, or key does not exist.
------------------------------------------------------------------------------------------------------------------------

Returns if field is an existing field in the hash stored at key.
--]]
function COMMAND_PARSE.HEXISTS(key, field)
    return _common_parse("HEXISTS", key, field)
end
function RESPONSE_PARSE.HEXISTS(r)
    return r == 1
end

--[[
------------------------------------------------------------------------------------------------------------------------
HGET key field
Available since 2.0.0
Time complexity: O(1)
Bulk string reply: the value associated with field, or nil when field is not present in the hash or key does not exist.
------------------------------------------------------------------------------------------------------------------------

Returns the value associated with field in the hash stored at key.
--]]
function COMMAND_PARSE.HGET(key, field)
    return _common_parse("HGET", key, field)
end

--[[
------------------------------------------------------------------------------------------------------------------------
HGETALL key
Available since 2.0.0.
Time complexity: O(N) where N is the size of the hash.
Array reply: list of fields and their values stored in the hash, or an empty list when key does not exist.
------------------------------------------------------------------------------------------------------------------------

Returns all fields and values of the hash stored at key. In the returned value, every field name is followed by its value, so the length of the reply is twice the size of the hash.
--]]
function COMMAND_PARSE.HGETALL(key)
    return _common_parse("HGETALL", key)
end
function RESPONSE_PARSE.HGETALL(r)
    local t = {}
    for idx = 1, #r, 2 do
        t[r[idx]] = r[idx+1]
    end
    return t
end

--[[
------------------------------------------------------------------------------------------------------------------------
HINCRBY key field increment
Available since 2.0.0.
Time complexity: O(1)
Integer reply: the value at field after the increment operation.
------------------------------------------------------------------------------------------------------------------------

Increments the number stored at field in the hash stored at key by increment.
If key does not exist, a new key holding a hash is created.
If field does not exist the value is set to 0 before the operation is performed.

The range of values supported by HINCRBY is limited to 64 bit signed integers.
--]]
function COMMAND_PARSE.HINCRBY(key, field, increment)
    return _common_parse("HINCRBY", key, field, increment)
end

--[[
------------------------------------------------------------------------------------------------------------------------
HINCRBYFLOAT key field increment
Available since 2.6.0.
Time complexity: O(1)
Bulk string reply: the value of field after the increment.
------------------------------------------------------------------------------------------------------------------------

Increment the specified field of a hash stored at key, and representing a floating point number, by the specified increment.
If the increment value is negative, the result is to have the hash field value decremented instead of incremented.
If the field does not exist, it is set to 0 before performing the operation.
An error is returned if one of the following conditions occur:
  - The field contains a value of the wrong type (not a string).
  - The current field content or the specified increment are not parsable as a double precision floating point number.

The exact behavior of this command is identical to the one of the INCRBYFLOAT command, please refer to the documentation of INCRBYFLOAT for further information.
--]]
function COMMAND_PARSE.HINCRBYFLOAT(key, field, increment)
    return _common_parse("HINCRBYFLOAT", key, field, increment)
end
function RESPONSE_PARSE.HINCRBYFLOAT(r)
    return tonumber(r)
end

--[[
------------------------------------------------------------------------------------------------------------------------
HKEYS key
Available since 2.0.0.
Time complexity: O(N) where N is the size of the hash.
Array reply: list of fields in the hash, or an empty list when key does not exist.
------------------------------------------------------------------------------------------------------------------------

Returns all field names in the hash stored at key.
--]]
function COMMAND_PARSE.HKEYS(key)
    return _common_parse("HKEYS", key)
end

--[[
------------------------------------------------------------------------------------------------------------------------
HLEN key
Available since 2.0.0.
Time complexity: O(1)
Integer reply: number of fields in the hash, or 0 when key does not exist.
------------------------------------------------------------------------------------------------------------------------

Returns the number of fields contained in the hash stored at key.
--]]
function COMMAND_PARSE.HLEN(key)
    return _common_parse("HLEN", key)
end

--[[
------------------------------------------------------------------------------------------------------------------------
HMGET key field [field ...]
Available since 2.0.0.
Time complexity: O(N) where N is the number of fields being requested.
Array reply: list of values associated with the given fields, in the same order as they are requested.
------------------------------------------------------------------------------------------------------------------------

Returns the values associated with the specified fields in the hash stored at key.

For every field that does not exist in the hash, a nil value is returned.
Because non-existing keys are treated as empty hashes, running HMGET against a non-existing key will return a list of nil values.
--]]
function COMMAND_PARSE.HMGET(key, field1, ...)
    return _common_parse("HMGET", key, field1, ...)
end

--[[
------------------------------------------------------------------------------------------------------------------------
HMSET key field value [field value ...]
Available since 2.0.0.
Time complexity: O(N) where N is the number of fields being set.
Simple string reply
As per Redis 4.0.0, HMSET is considered deprecated. Please use HSET in new code.
------------------------------------------------------------------------------------------------------------------------

Sets the specified fields to their respective values in the hash stored at key.
This command overwrites any specified fields already existing in the hash.
If key does not exist, a new key holding a hash is created.
--]]
function COMMAND_PARSE.HMSET(key, fvPairs)
    local params = { key }
    for f, v in pairs(fvPairs) do
        table.insert(params, f)
        table.insert(params, v)
    end

    params.n = #params
    return make_request("HMSET", params)
end
function RESPONSE_PARSE.HMSET(r)
    return true
end

--[[
------------------------------------------------------------------------------------------------------------------------
HSCAN key cursor [MATCH pattern] [COUNT count]
Available since 2.8.0.
Time complexity: O(1) for every call. O(N) for a complete iteration, including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection..
------------------------------------------------------------------------------------------------------------------------

See SCAN for HSCAN documentation.
--]]
function COMMAND_PARSE.HSCAN(key, cursor, pattern, count)
    cursor = math.tointeger(cursor) or 0
    count = math.tointeger(count)
    local params = { key, cursor }
    if pattern then
        table.insert(params, "MATCH")
        table.insert(params, pattern)
    end
    if count then
        table.insert(params, "COUNT")
        table.insert(params, count)
    end

    params.n = #params
    return make_request("HSCAN", params)
end

--[[
------------------------------------------------------------------------------------------------------------------------
HSET key field value [field value ...]
Available since 2.0.0.
Time complexity: O(1) for each field/value pair added, so O(N) to add N field/value pairs when the command is called with multiple field/value pairs.
Integer reply: The number of fields that were added.
As of Redis 4.0.0, HSET is variadic and allows for multiple field/value pairs.
------------------------------------------------------------------------------------------------------------------------

Sets field in the hash stored at key to value.
If key does not exist, a new key holding a hash is created.
If field already exists in the hash, it is overwritten.
--]]
function COMMAND_PARSE.HSET(key, fvPairs)
    local params = { key }
    for f, v in pairs(fvPairs) do
        table.insert(params, f)
        table.insert(params, v)
    end

    params.n = #params
    return make_request("HSET", params)
end

--[[
------------------------------------------------------------------------------------------------------------------------
HSETNX key field value
Available since 2.0.0.
Time complexity: O(1)
Integer reply, specifically:
  - 1 if field is a new field in the hash and value was set.
  - 0 if field already exists in the hash and no operation was performed.
------------------------------------------------------------------------------------------------------------------------

Sets field in the hash stored at key to value, only if field does not yet exist. If key does not exist, a new key holding a hash is created. If field already exists, this operation has no effect.
--]]
function COMMAND_PARSE.HSETNX(key, field, value)
    return _common_parse("HSETNX", key, field, value)
end
function RESPONSE_PARSE.HSETNX(r)
    return r == 1
end

--[[
------------------------------------------------------------------------------------------------------------------------
HSTRLEN key field
Available since 3.2.0.
Time complexity: O(1)
Integer reply: the string length of the value associated with field, or zero when field is not present in the hash or key does not exist at all.
------------------------------------------------------------------------------------------------------------------------

Returns the string length of the value associated with field in the hash stored at key. If the key or the field do not exist, 0 is returned.
--]]
function COMMAND_PARSE.HSTRLEN(key, field)
    return _common_parse("HSTRLEN", key, field)
end

--[[
------------------------------------------------------------------------------------------------------------------------
HVALS key
Available since 2.0.0.
Time complexity: O(N) where N is the size of the hash.
Array reply: list of values in the hash, or an empty list when key does not exist.
------------------------------------------------------------------------------------------------------------------------

Returns all values in the hash stored at key.
--]]
function COMMAND_PARSE.HVALS(key)
    return _common_parse("HVALS", key)
end

--endregion


--region List Command

--[[
------------------------------------------------------------------------------------------------------------------------
BLPOP key [key ...] timeout
Available since 2.0.0.
Time complexity: O(1)
Array reply: specifically:
  - A nil multi-bulk when no element could be popped and the timeout expired.
  - A two-element multi-bulk with the first element being the name of the key where an element was popped and the second element being the value of the popped element.
------------------------------------------------------------------------------------------------------------------------

BLPOP is a blocking list pop primitive.
It is the blocking version of LPOP because it blocks the connection when there are no elements to pop from any of the given lists.
An element is popped from the head of the first list that is non-empty, with the given keys being checked in the order that they are given.

When BLPOP is called, if at least one of the specified keys contains a non-empty list,
an element is popped from the head of the list and returned to the caller together with the key it was popped from.

If none of the specified keys exist, BLPOP blocks the connection until another client performs an LPUSH or RPUSH operation against one of the keys.

Once new data is present on one of the lists, the client returns with the name of the key unblocking it and the popped value.

When BLPOP causes a client to block and a non-zero timeout is specified,
the client will unblock returning a nil multi-bulk value when the specified timeout has expired without a push operation against at least one of the specified keys.
--]]
function COMMAND_PARSE.BLPOP(timeout, key1, ...)
    timeout = math.tointeger(timeout) or 0
    local params = { ... }
    table.insert(params, 1, key1)
    table.insert(params, timeout)

    params.n = #params
    return make_request("BLPOP", params)
end

--[[
------------------------------------------------------------------------------------------------------------------------
BRPOP key [key ...] timeout
Available since 2.0.0.
Time complexity: O(1)
Array reply: specifically:
  - A nil multi-bulk when no element could be popped and the timeout expired.
  - A two-element multi-bulk with the first element being the name of the key where an element was popped and the second element being the value of the popped element.
------------------------------------------------------------------------------------------------------------------------

BRPOP is a blocking list pop primitive.
It is the blocking version of RPOP because it blocks the connection when there are no elements to pop from any of the given lists.
An element is popped from the tail of the first list that is non-empty, with the given keys being checked in the order that they are given.

See the BLPOP documentation for the exact semantics,
since BRPOP is identical to BLPOP with the only difference being that it pops elements from the tail of a list instead of popping from the head.
--]]
function COMMAND_PARSE.BRPOP(timeout, key1, ...)
    timeout = math.tointeger(timeout) or 0
    local params = { ... }
    table.insert(params, 1, key1)
    table.insert(params, timeout)

    params.n = #params
    return make_request("BRPOP", params)
end

--[[
------------------------------------------------------------------------------------------------------------------------
BRPOPLPUSH source destination timeout
Available since 2.2.0.
Time complexity: O(1)
Bulk string reply: the element being popped from source and pushed to destination. If timeout is reached, a Null reply is returned.
------------------------------------------------------------------------------------------------------------------------

BRPOPLPUSH is the blocking variant of RPOPLPUSH. When source contains elements, this command behaves exactly like RPOPLPUSH.
When used inside a MULTI/EXEC block, this command behaves exactly like RPOPLPUSH.
When source is empty, Redis will block the connection until another client pushes to it or until timeout is reached.
A timeout of zero can be used to block indefinitely.

See RPOPLPUSH for more information.
--]]
function COMMAND_PARSE.BRPOPLPUSH(source, destination, timeout)
    timeout = math.tointeger(timeout) or 0
    return _common_parse("BRPOPLPUSH", source, destination, timeout)
end

--[[
------------------------------------------------------------------------------------------------------------------------
LINDEX key index
Available since 1.0.0.
Time complexity: O(N) where N is the number of elements to traverse to get to the element at index. This makes asking for the first or the last element of the list O(1).
Bulk string reply: the requested element, or nil when index is out of range.
------------------------------------------------------------------------------------------------------------------------

Returns the element at index index in the list stored at key.
The index is zero-based, so 0 means the first element, 1 the second element and so on.
Negative indices can be used to designate elements starting at the tail of the list. Here, -1 means the last element, -2 means the penultimate and so forth.

When the value at key is not a list, an error is returned.
--]]
function COMMAND_PARSE.LINDEX(key, index)
    index = math.tointeger(index)
    return _common_parse("LINDEX", key, index)
end

--[[
------------------------------------------------------------------------------------------------------------------------
LINSERT key BEFORE|AFTER pivot element
Available since 2.2.0.
Time complexity: O(N) where N is the number of elements to traverse before seeing the value pivot. This means that inserting somewhere on the left end on the list (head) can be considered O(1) and inserting somewhere on the right end (tail) is O(N).
Integer reply: the length of the list after the insert operation, or -1 when the value pivot was not found.
------------------------------------------------------------------------------------------------------------------------

Inserts element in the list stored at key either before or after the reference value pivot.

When key does not exist, it is considered an empty list and no operation is performed.

An error is returned when key exists but does not hold a list value.
--]]
function COMMAND_PARSE.LINSERT(key, pivot, element, insertBefore)
    local params = { key }
    if insertBefore then
        table.insert(params, "BEFORE")
    else
        table.insert(params, "AFTER")
    end
    table.insert(params, pivot)
    table.insert(params, element)

    params.n = #params
    return make_request("LINSERT", params)
end

--[[
------------------------------------------------------------------------------------------------------------------------
LLEN key
Available since 1.0.0.
Time complexity: O(1)
Integer reply: the length of the list at key.
------------------------------------------------------------------------------------------------------------------------

Returns the length of the list stored at key.
If key does not exist, it is interpreted as an empty list and 0 is returned.
An error is returned when the value stored at key is not a list.
--]]
function COMMAND_PARSE.LLEN(key)
    return _common_parse("LLEN", key)
end

--[[
------------------------------------------------------------------------------------------------------------------------
LPOP key
Available since 1.0.0.
Time complexity: O(1)
Bulk string reply: the value of the first element, or nil when key does not exist.
------------------------------------------------------------------------------------------------------------------------

Removes and returns the first element of the list stored at key.
--]]
function COMMAND_PARSE.LPOP(key)
    return _common_parse("LPOP", key)
end

--[[
------------------------------------------------------------------------------------------------------------------------
LPUSH key element [element ...]
Available since 1.0.0.
  - >= 2.4: Accepts multiple element arguments. In Redis versions older than 2.4 it was possible to push a single value per command.
Time complexity: O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
Integer reply: the length of the list after the push operations.
------------------------------------------------------------------------------------------------------------------------

Insert all the specified values at the head of the list stored at key.
If key does not exist, it is created as empty list before performing the push operations.
When key holds a value that is not a list, an error is returned.

It is possible to push multiple elements using a single command call just specifying multiple arguments at the end of the command.
Elements are inserted one after the other to the head of the list, from the leftmost element to the rightmost element.
--]]
function COMMAND_PARSE.LPUSH(key, element1, ...)
    return _common_parse("LPUSH", key, element1, ...)
end

--[[
------------------------------------------------------------------------------------------------------------------------
LPUSHX key element [element ...]
Available since 2.2.0.
  - >= 4.0: Accepts multiple element arguments. In Redis versions older than 4.0 it was possible to push a single value per command.
Time complexity: O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
Integer reply: the length of the list after the push operation.
------------------------------------------------------------------------------------------------------------------------

Inserts specified values at the head of the list stored at key, only if key already exists and holds a list.
In contrary to LPUSH, no operation will be performed when key does not yet exist.
--]]
function COMMAND_PARSE.LPUSHX(key, element1, ...)
    return _common_parse("LPUSHX", key, element1, ...)
end

--[[
------------------------------------------------------------------------------------------------------------------------
LRANGE key start stop
Available since 1.0.0.
Time complexity: O(S+N) where S is the distance of start offset from HEAD for small lists, from nearest end (HEAD or TAIL) for large lists; and N is the number of elements in the specified range.
Array reply: list of elements in the specified range.
------------------------------------------------------------------------------------------------------------------------

Returns the specified elements of the list stored at key.
The offsets start and stop are zero-based indexes, with 0 being the first element of the list (the head of the list), 1 being the next element and so on.
These offsets can also be negative numbers indicating offsets starting at the end of the list.

Out of range indexes will not produce an error.
If start is larger than the end of the list, an empty list is returned.
If stop is larger than the actual end of the list, Redis will treat it like the last element of the list.
--]]
function COMMAND_PARSE.LRANGE(key, start, stop)
    start = math.tointeger(start)
    stop = math.tointeger(stop)
    return _common_parse("LRANGE", key, start, stop)
end

--[[
------------------------------------------------------------------------------------------------------------------------
LREM key count element
Available since 1.0.0.
Time complexity: O(N+M) where N is the length of the list and M is the number of elements removed.
Integer reply: the number of removed elements.
------------------------------------------------------------------------------------------------------------------------

Removes the first count occurrences of elements equal to element from the list stored at key.
The count argument influences the operation in the following ways:
  - count > 0: Remove elements equal to element moving from head to tail.
  - count < 0: Remove elements equal to element moving from tail to head.
  - count = 0: Remove all elements equal to element.

Note that non-existing keys are treated like empty lists, so when key does not exist, the command will always return 0.
--]]
function COMMAND_PARSE.LREM(key, element, count)
    count = math.tointeger(count)
    return _common_parse("LREM", key, count, element)
end

--[[
------------------------------------------------------------------------------------------------------------------------
LSET key index element
Available since 1.0.0.
Time complexity: O(N) where N is the length of the list. Setting either the first or the last element of the list is O(1).
Simple string reply
------------------------------------------------------------------------------------------------------------------------

Sets the list element at index to element. For more information on the index argument, see LINDEX.

An error is returned for out of range indexes.
--]]
function COMMAND_PARSE.LSET(key, index, element)
    index = math.tointeger(index)
    return _common_parse("LSET", key, index, element)
end
function RESPONSE_PARSE.LSET(r)
    return true
end

--[[
------------------------------------------------------------------------------------------------------------------------
LTRIM key start stop
Available since 1.0.0.
Time complexity: O(N) where N is the number of elements to be removed by the operation.
Simple string reply
------------------------------------------------------------------------------------------------------------------------

Trim an existing list so that it will contain only the specified range of elements specified.
Both start and stop are zero-based indexes, where 0 is the first element of the list (the head), 1 the next element and so on.

start and end can also be negative numbers indicating offsets from the end of the list, where -1 is the last element of the list, -2 the penultimate element and so on.

Out of range indexes will not produce an error:
if start is larger than the end of the list, or start > end, the result will be an empty list (which causes key to be removed).
If end is larger than the end of the list, Redis will treat it like the last element of the list.
--]]
function COMMAND_PARSE.LTRIM(key, start, stop)
    start = math.tointeger(start)
    stop = math.tointeger(stop)
    return _common_parse("LTRIM", key, start, stop)
end
function RESPONSE_PARSE.LTRIM(r)
    return true
end

--[[
------------------------------------------------------------------------------------------------------------------------
RPOP key
Available since 1.0.0.
Time complexity: O(1)
Bulk string reply: the value of the last element, or nil when key does not exist.
------------------------------------------------------------------------------------------------------------------------

Removes and returns the last element of the list stored at key.
--]]
function COMMAND_PARSE.RPOP(key)
    return _common_parse("RPOP", key)
end

--[[
------------------------------------------------------------------------------------------------------------------------
RPOPLPUSH source destination
Available since 1.2.0.
Time complexity: O(1)
Bulk string reply: the element being popped and pushed.
------------------------------------------------------------------------------------------------------------------------

Atomically returns and removes the last element (tail) of the list stored at source, and pushes the element at the first element (head) of the list stored at destination.

If source does not exist, the value nil is returned and no operation is performed.
If source and destination are the same, the operation is equivalent to removing the last element from the list and pushing it as first element of the list, so it can be considered as a list rotation command.
--]]
function COMMAND_PARSE.RPOPLPUSH(source, destination)
    return _common_parse("RPOPLPUSH", source, destination)
end

--[[
------------------------------------------------------------------------------------------------------------------------
RPUSH key element [element ...]
Available since 1.0.0.
  - >= 2.4: Accepts multiple element arguments. In Redis versions older than 2.4 it was possible to push a single value per command.
Time complexity: O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
Integer reply: the length of the list after the push operation.
------------------------------------------------------------------------------------------------------------------------

Insert all the specified values at the tail of the list stored at key.
If key does not exist, it is created as empty list before performing the push operation.
When key holds a value that is not a list, an error is returned.

It is possible to push multiple elements using a single command call just specifying multiple arguments at the end of the command.
Elements are inserted one after the other to the tail of the list, from the leftmost element to the rightmost element.
--]]
function COMMAND_PARSE.RPUSH(key, element1, ...)
    return _common_parse("RPUSH", key, element1, ...)
end

--[[
------------------------------------------------------------------------------------------------------------------------
RPUSHX key element [element ...]
Available since 2.2.0.
  - >= 4.0: Accepts multiple element arguments. In Redis versions older than 4.0 it was possible to push a single value per command.
Time complexity: O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
Integer reply: the length of the list after the push operation.
------------------------------------------------------------------------------------------------------------------------

Inserts specified values at the tail of the list stored at key, only if key already exists and holds a list.
In contrary to RPUSH, no operation will be performed when key does not yet exist.
--]]
function COMMAND_PARSE.RPUSHX(key, element1, ...)
    return _common_parse("RPUSHX", key, element1, ...)
end

--endregion


--region Set Command

--[[
------------------------------------------------------------------------------------------------------------------------
SADD key member [member ...]
Available since 1.0.0.
  - >= 2.4: Accepts multiple member arguments. Redis versions before 2.4 are only able to add a single member per call.
Time complexity: O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
Integer reply: the number of elements that were added to the set, not including all the elements already present into the set.
------------------------------------------------------------------------------------------------------------------------

Add the specified members to the set stored at key.
Specified members that are already a member of this set are ignored.
If key does not exist, a new set is created before adding the specified members.

An error is returned when the value stored at key is not a set.
--]]
function COMMAND_PARSE.SADD(key, member1, ...)
    return _common_parse("SADD", key, member1, ...)
end

--[[
------------------------------------------------------------------------------------------------------------------------
SCARD key
Available since 1.0.0.
Time complexity: O(1)
Integer reply: the cardinality (number of elements) of the set, or 0 if key does not exist.
------------------------------------------------------------------------------------------------------------------------

Returns the set cardinality (number of elements) of the set stored at key.
--]]
function COMMAND_PARSE.SCARD(key)
    return _common_parse("SCARD", key)
end

--[[
------------------------------------------------------------------------------------------------------------------------
SDIFF key [key ...]
Available since 1.0.0.
Time complexity: O(N) where N is the total number of elements in all given sets.
Array reply: list with members of the resulting set.
------------------------------------------------------------------------------------------------------------------------

Returns the members of the set resulting from the difference between the first set and all the successive sets.
--]]
function COMMAND_PARSE.SDIFF(key1, ...)
    return _common_parse("SDIFF", key1, ...)
end

--[[
------------------------------------------------------------------------------------------------------------------------
SDIFFSTORE destination key [key ...]
Available since 1.0.0.
Time complexity: O(N) where N is the total number of elements in all given sets.
Integer reply: the number of elements in the resulting set.
------------------------------------------------------------------------------------------------------------------------

This command is equal to SDIFF, but instead of returning the resulting set, it is stored in destination.
If destination already exists, it is overwritten.
--]]
function COMMAND_PARSE.SDIFFSTORE(destination, key1, ...)
    return _common_parse("SDIFFSTORE", destination, key1, ...)
end

--[[
------------------------------------------------------------------------------------------------------------------------
SINTER key [key ...]
Available since 1.0.0.
Time complexity: O(N*M) worst case where N is the cardinality of the smallest set and M is the number of sets.
Array reply: list with members of the resulting set.
------------------------------------------------------------------------------------------------------------------------

Returns the members of the set resulting from the intersection of all the given sets.
Keys that do not exist are considered to be empty sets.
With one of the keys being an empty set, the resulting set is also empty (since set intersection with an empty set always results in an empty set).
--]]
function COMMAND_PARSE.SINTER(key1, ...)
    return _common_parse("SINTER", key1, ...)
end

--[[
------------------------------------------------------------------------------------------------------------------------
SINTERSTORE destination key [key ...]
Available since 1.0.0.
Time complexity: O(N*M) worst case where N is the cardinality of the smallest set and M is the number of sets.
Integer reply: the number of elements in the resulting set.
------------------------------------------------------------------------------------------------------------------------

This command is equal to SINTER, but instead of returning the resulting set, it is stored in destination.
If destination already exists, it is overwritten.
--]]
function COMMAND_PARSE.SINTERSTORE(destination, key1, ...)
    return _common_parse("SINTERSTORE", destination, key1, ...)
end

--[[
------------------------------------------------------------------------------------------------------------------------
SISMEMBER key member
Available since 1.0.0.
Time complexity: O(1)
Integer reply, specifically:
  - 1 if the element is a member of the set.
  - 0 if the element is not a member of the set, or if key does not exist.
------------------------------------------------------------------------------------------------------------------------

Returns if member is a member of the set stored at key.
--]]
function COMMAND_PARSE.SISMEMBER(key, member)
    return _common_parse("SISMEMBER", key, member)
end
function RESPONSE_PARSE.SISMEMBER(r)
    return r == 1
end

--[[
------------------------------------------------------------------------------------------------------------------------
SMEMBERS key
Available since 1.0.0.
Time complexity: O(N) where N is the set cardinality.
Array reply: all elements of the set.
------------------------------------------------------------------------------------------------------------------------

Returns all the members of the set value stored at key.
This has the same effect as running SINTER with one argument key.
--]]
function COMMAND_PARSE.SMEMBERS(key)
    return _common_parse("SMEMBERS", key)
end

--[[
------------------------------------------------------------------------------------------------------------------------
SMOVE source destination member
Available since 1.0.0.
Time complexity: O(1)
Integer reply, specifically:
  - 1 if the element is moved.
  - 0 if the element is not a member of source and no operation was performed.
------------------------------------------------------------------------------------------------------------------------

Move member from the set at source to the set at destination.
This operation is atomic. In every given moment the element will appear to be a member of source or destination for other clients.

If the source set does not exist or does not contain the specified element, no operation is performed and 0 is returned.
Otherwise, the element is removed from the source set and added to the destination set.
When the specified element already exists in the destination set, it is only removed from the source set.

An error is returned if source or destination does not hold a set value.
--]]
function COMMAND_PARSE.SMOVE(source, destination, member)
    return _common_parse("SMOVE", source, destination, member)
end
function RESPONSE_PARSE.SMOVE(r)
    return r == 1
end

--[[
------------------------------------------------------------------------------------------------------------------------
SPOP key [count]
Available since 1.0.0.
  - The count argument is available since version 3.2.
Time complexity: O(1)
Bulk string reply: the removed element, or nil when key does not exist.
------------------------------------------------------------------------------------------------------------------------

Removes and returns one or more random elements from the set value store at key.

This operation is similar to SRANDMEMBER, that returns one or more random elements from a set but does not remove it.
--]]
function COMMAND_PARSE.SPOP(key, count)
    count = math.tointeger(count)
    if count then
        return _common_parse("SPOP", key, count)
    end
    return _common_parse("SPOP", key)
end

--[[
------------------------------------------------------------------------------------------------------------------------
SRANDMEMBER key [count]
Available since 1.0.0.
Time complexity: Without the count argument O(1), otherwise O(N) where N is the absolute value of the passed count.
Bulk string reply: without the additional count argument the command returns a Bulk Reply with the randomly selected element, or nil when key does not exist.
Array reply: when the additional count argument is passed the command returns an array of elements, or an empty array when key does not exist.
------------------------------------------------------------------------------------------------------------------------

When called with just the key argument, return a random element from the set value stored at key.

Starting from Redis version 2.6, when called with the additional count argument, return an array of count distinct elements if count is positive.
If called with a negative count the behavior changes and the command is allowed to return the same element multiple times.
In this case the number of returned elements is the absolute value of the specified count.

When called with just the key argument, the operation is similar to SPOP,
however while SPOP also removes the randomly selected element from the set,
SRANDMEMBER will just return a random element without altering the original set in any way.
--]]
function COMMAND_PARSE.SRANDMEMBER(key, count)
    count = math.tointeger(count)
    if count then
        return _common_parse("SRANDMEMBER", key, count)
    end
    return _common_parse("SRANDMEMBER", key)
end

--[[
------------------------------------------------------------------------------------------------------------------------
SREM key member [member ...]
Available since 1.0.0.
  - >= 2.4: Accepts multiple member arguments. Redis versions older than 2.4 can only remove a set member per call.
Time complexity: O(N) where N is the number of members to be removed.
Integer reply: the number of members that were removed from the set, not including non existing members.
------------------------------------------------------------------------------------------------------------------------

Remove the specified members from the set stored at key.
Specified members that are not a member of this set are ignored.
If key does not exist, it is treated as an empty set and this command returns 0.

An error is returned when the value stored at key is not a set.
--]]
function COMMAND_PARSE.SREM(key, member1, ...)
    return _common_parse("SREM", key, member1, ...)
end

--[[
------------------------------------------------------------------------------------------------------------------------
SSCAN key cursor [MATCH pattern] [COUNT count]
Available since 2.8.0.
Time complexity: O(1) for every call. O(N) for a complete iteration, including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection..
------------------------------------------------------------------------------------------------------------------------

See SCAN for SSCAN documentation.
--]]
function COMMAND_PARSE.SSCAN(key, cursor, pattern, count)
    cursor = math.tointeger(cursor) or 0
    count = math.tointeger(count)
    local params = { key, cursor }
    if pattern then
        table.insert(params, "MATCH")
        table.insert(params, pattern)
    end
    if count then
        table.insert(params, "COUNT")
        table.insert(params, count)
    end

    params.n = #params
    return make_request("SSCAN", params)
end

--[[
------------------------------------------------------------------------------------------------------------------------
SUNION key [key ...]
Available since 1.0.0.
Time complexity: O(N) where N is the total number of elements in all given sets.
Array reply: list with members of the resulting set.
------------------------------------------------------------------------------------------------------------------------

Returns the members of the set resulting from the union of all the given sets.
Keys that do not exist are considered to be empty sets.
--]]
function COMMAND_PARSE.SUNION(key1, ...)
    return _common_parse("SUNION", key1, ...)
end

--[[
------------------------------------------------------------------------------------------------------------------------
SUNIONSTORE destination key [key ...]
Available since 1.0.0.
Time complexity: O(N) where N is the total number of elements in all given sets.
Integer reply: the number of elements in the resulting set.
------------------------------------------------------------------------------------------------------------------------

This command is equal to SUNION, but instead of returning the resulting set, it is stored in destination.
If destination already exists, it is overwritten.
--]]
function COMMAND_PARSE.SUNIONSTORE(destination, key1, ...)
    return _common_parse("SUNIONSTORE", destination, key1, ...)
end

--endregion


--region Transaction Command

--[[
------------------------------------------------------------------------------------------------------------------------
MULTI
Available since 1.2.0.
Simple string reply: always OK.
------------------------------------------------------------------------------------------------------------------------

Marks the start of a transaction block. Subsequent commands will be queued for atomic execution using EXEC.
--]]
function TRANSACTION_PARSE.MULTI()
    return _common_parse("MULTI")
end
function RESPONSE_PARSE.MULTI(r)
    return true
end

--[[
------------------------------------------------------------------------------------------------------------------------
EXEC
Available since 1.2.0.
Array reply: each element being the reply to each of the commands in the atomic transaction.
When using WATCH, EXEC can return a Null reply if the execution was aborted.
------------------------------------------------------------------------------------------------------------------------

Executes all previously queued commands in a transaction and restores the connection state to normal.

When using WATCH, EXEC will execute commands only if the watched keys were not modified, allowing for a check-and-set mechanism.
--]]
function TRANSACTION_PARSE.EXEC()
    return _common_parse("EXEC")
end

--endregion


--region Publish/Subscribe Command

--[[
------------------------------------------------------------------------------------------------------------------------
PUBLISH channel message
Available since 2.0.0.
Time complexity: O(N+M) where N is the number of clients subscribed to the receiving channel and M is the total number of subscribed patterns (by any client).
Integer reply: the number of clients that received the message.
------------------------------------------------------------------------------------------------------------------------

Posts a message to the given channel.
--]]
function COMMAND_PARSE.PUBLISH(channel, message)
    return _common_parse("PUBLISH", channel, message)
end

--[[
------------------------------------------------------------------------------------------------------------------------
SUBSCRIBE channel [channel ...]
Available since 2.0.0.
Time complexity: O(N) where N is the number of channels to subscribe to.
------------------------------------------------------------------------------------------------------------------------

Subscribes the client to the specified channels.

Once the client enters the subscribed state it is not supposed to issue any other commands, except for additional SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, PING and QUIT commands.
--]]
function PUBSUB_PARSE.SUBSCRIBE(channel1, ...)
    return _common_parse("SUBSCRIBE", channel1, ...)
end

--[==[
------------------------------------------------------------------------------------------------------------------------
UNSUBSCRIBE [channel [channel ...]]
Available since 2.0.0.
Time complexity: O(N) where N is the number of clients already subscribed to a channel.
------------------------------------------------------------------------------------------------------------------------

Unsubscribes the client from the given channels, or from all of them if none is given.

When no channels are specified, the client is unsubscribed from all the previously subscribed channels.
In this case, a message for every unsubscribed channel will be sent to the client.
--]==]
function PUBSUB_PARSE.UNSUBSCRIBE(...)
    return _common_parse("UNSUBSCRIBE", ...)
end

--[[
------------------------------------------------------------------------------------------------------------------------
PSUBSCRIBE pattern [pattern ...]
Available since 2.0.0.
Time complexity: O(N) where N is the number of patterns the client is already subscribed to.
------------------------------------------------------------------------------------------------------------------------

Subscribes the client to the given patterns.

Supported glob-style patterns:
  - h?llo subscribes to hello, hallo and hxllo
  - h*llo subscribes to hllo and heeeello
  - h[ae]llo subscribes to hello and hallo, but not hillo
Use \ to escape special characters if you want to match them verbatim.
--]]
function PUBSUB_PARSE.PSUBSCRIBE(pattern1, ...)
    return _common_parse("PSUBSCRIBE", pattern1, ...)
end

--[==[
------------------------------------------------------------------------------------------------------------------------
PUNSUBSCRIBE [pattern [pattern ...]]
Available since 2.0.0.
Time complexity: O(N+M) where N is the number of patterns the client is already subscribed and M is the number of total patterns subscribed in the system (by any client).
------------------------------------------------------------------------------------------------------------------------

Unsubscribes the client from the given patterns, or from all of them if none is given.

When no patterns are specified, the client is unsubscribed from all the previously subscribed patterns.
In this case, a message for every unsubscribed pattern will be sent to the client.
--]==]
function PUBSUB_PARSE.PUNSUBSCRIBE(...)
    return _common_parse("PUNSUBSCRIBE", ...)
end

--endregion


---@class RedisClientBase
local RedisClientBase = class()
function RedisClientBase:ctor(conf)
    self.host       = conf.host
    self.port       = conf.port or 6379
    self.dbindex    = conf.dbindex or 0
    self.password   = conf.password

    self.__sock = socketchannel.channel {
        host = self.host,
        port = self.port,
        auth = function(fd)
            if self.password then
                self.__sock:request(COMMAND_PARSE["AUTH"](self.password), parse_response)
            end
            self.__sock:request(COMMAND_PARSE["SELECT"](self.dbindex), parse_response)
        end,
        nodelay = true,
        overload = conf.overload,
    }
    self.__sock:connect(true)
end
function RedisClientBase:tostring()
    return string.format("[RedisClientBase: %s:%s]", self.host, self.port)
end

function RedisClientBase:disconnect()
    if self.__sock then
        local fd = self.__sock
        self.__sock = false
        fd:close()
    end
end


---@class RedisClient
local RedisClient = class(RedisClientBase)
function RedisClient:ctor(conf) end
function RedisClient:tostring()
    return string.format("[RedisClient: %s:%s]", self.host, self.port)
end
redis.RedisClient = RedisClient

function RedisClient:new_command_buffer()
    return { requestArray = {}, cmds = {}, cnt = 0, err = false }
end

local function __do_buffer_command(parsers, buffer, cmd, ...)
    if buffer.err then
        return false, "buffer has errors"
    end

    local command_parser = parsers[cmd]
    if not command_parser then
        buffer.err = true
        return false, "unsupported command"
    end

    local requestArray = command_parser(...)
    for _, v in ipairs(requestArray) do
        table.insert(buffer.requestArray, v)
    end

    table.insert(buffer.cmds, cmd)

    buffer.cnt = buffer.cnt + 1
    return true
end

function RedisClient:transaction_command(buffer, cmd, ...)
    return __do_buffer_command(TRANSACTION_PARSE, buffer, cmd, ...)
end

function RedisClient:buffer_command(buffer, cmd, ...)
    return __do_buffer_command(COMMAND_PARSE, buffer, cmd, ...)
end

function RedisClient:command_execute(cmd, ...)
    local command_parser = COMMAND_PARSE[cmd]
    if not command_parser then
        return false, "unsupported command"
    end

    local success, r = pcall(self.__sock.request, self.__sock, command_parser(...), parse_response)
    if not success then
        return false, r
    end

    local response_parser = RESPONSE_PARSE[cmd]
    if response_parser then
        r = response_parser(r)
    end
    return true, r
end

function RedisClient:pipeline_execute(buffer)
    if buffer.err then
        return false, "buffer has errors"
    end

    if buffer.cnt <= 0 then
        return false, "no any pipeline commands"
    end

    return pcall(self.__sock.request, self.__sock, buffer.requestArray, function(fd)
        local pipelineRet = {}
        for idx = 1, buffer.cnt do
            local success, r = parse_response(fd)
            if not success then
                table.insert(pipelineRet, { false, r })
            else
                local response_parser = RESPONSE_PARSE[buffer.cmds[idx]]
                if response_parser then
                    r = response_parser(r)
                end
                table.insert(pipelineRet, { true, r })
            end
        end
        return true, pipelineRet
    end)
end

function RedisClient:transaction_execute(buffer)
    if buffer.err then
        return false, "buffer has errors"
    end

    if buffer.cnt <= 2 then
        return false, "no any transaction commands"
    end

    return pcall(self.__sock.request, self.__sock, buffer.requestArray, function(fd)
        local firstErr
        for _ = 1, buffer.cnt-1 do
            local success, r = parse_response(fd)
            if not success and not firstErr then
                firstErr = r
            end
        end
        local success, transactionRet = parse_response(fd)

        if firstErr then
            return false, firstErr
        end
        if not success then
            return false, transactionRet
        end

        local exec_parser = RESPONSE_PARSE[buffer.cmds[buffer.cnt]]
        if exec_parser then
            transactionRet = exec_parser(transactionRet)
        end
        for idx = 2, buffer.cnt-1 do
            local response_parser = RESPONSE_PARSE[buffer.cmds[idx]]
            if response_parser then
                transactionRet[idx-1] = response_parser(transactionRet[idx-1])
            end
        end
        return success, transactionRet
    end)
end

---@class RedisClientSubscribe
local DEFAULT_SUBSCRIBE_CALLBACK_KEY = {}
local subscribeCallbacks = {}
local subscribeResponse
local DEFAULT_PSUBSCRIBE_CALLBACK_KEY = {}
local psubscribeCallbacks = {}
local psubscribeResponse

local function handle_subscribe(r)
    local kind, channel, cnt = r[1], r[2], r[3]
    if subscribeResponse then
        subscribeResponse(kind, channel, cnt)
    end
end

local function handle_message(r)
    local channel, message = r[2], r[3]
    local callback = subscribeCallbacks[channel]
    if not callback then
        callback = subscribeCallbacks[DEFAULT_SUBSCRIBE_CALLBACK_KEY]
    end
    if callback then
        callback(channel, message)
    end
end

local function handle_psubscribe(r)
    local kind, pattern, cnt = r[1], r[2], r[3]
    if psubscribeResponse then
        psubscribeResponse(kind, pattern, cnt)
    end
end

local function handle_pmessage(r)
    local pattern, channel, message = r[2], r[3], r[4]
    local callback = psubscribeCallbacks[pattern]
    if not callback then
        callback = psubscribeCallbacks[DEFAULT_PSUBSCRIBE_CALLBACK_KEY]
    end
    if callback then
        callback(pattern, channel, message)
    end
end

local subscribeDispatch = {
    subscribe       = handle_subscribe,
    unsubscribe     = handle_subscribe,
    message         = handle_message,
    psubscribe      = handle_psubscribe,
    punsubscribe    = handle_psubscribe,
    pmessage        = handle_pmessage
}

local function subscribe_receive(self)
    while self.__sock do
        local success, r = pcall(self.__sock.response, self.__sock, parse_response)
        if success then
            local handle_func = subscribeDispatch[r[1]]
            if handle_func then
                handle_func(r)
            end
        end
    end
end

local RedisClientSubscribe = class(RedisClientBase)
function RedisClientSubscribe:ctor(conf)
    skynet.fork(subscribe_receive, self)
end
function RedisClientSubscribe:tostring()
    return string.format("[RedisClientSubscribe: %s:%s]", self.host, self.port)
end
redis.RedisClientSubscribe = RedisClientSubscribe

function RedisClientSubscribe:set_subscribe_response(callback)
    subscribeResponse = callback
end

function RedisClientSubscribe:set_subscribe_callback(callback, channel)
    if channel then
        subscribeCallbacks[channel] = callback
    else
        subscribeCallbacks[DEFAULT_SUBSCRIBE_CALLBACK_KEY] = callback
    end
end

function RedisClientSubscribe:set_psubscribe_response(callback)
    psubscribeResponse = callback
end

function RedisClientSubscribe:set_psubscribe_callback(callback, pattern)
    if pattern then
        psubscribeCallbacks[pattern] = callback
    else
        psubscribeCallbacks[DEFAULT_PSUBSCRIBE_CALLBACK_KEY] = callback
    end
end

function RedisClientSubscribe:subscribe(cmd, ...)
    local subscribe_parser = PUBSUB_PARSE[cmd]
    if not subscribe_parser then
        return false, "unsupported subscribe"
    end

    return pcall(self.__sock.request, self.__sock, subscribe_parser(...))
end


return redis