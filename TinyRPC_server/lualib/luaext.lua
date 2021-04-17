-----------------
-- table扩展start

-- 返回table的大小
table.size = function(t)
    local count = 0
    for _ in pairs(t) do
        count = count + 1
    end
    return count
end

-- 判断table是否为空
table.empty = function(t)
    if type(t) ~= "table" then
        return true
    end
    return next(t) == nil
end

-- 返回table所有的键 { k1, k2, ..., kn }
table.keys = function(t)
    local keys = {}
    for k, _ in pairs(t) do
        table.insert(keys, k)
    end
    return keys
end

-- 返回table所有的值 { v1, v2, ..., vn }
table.values = function(t)
    local values = {}
    for _, v in pairs(t) do
        table.insert(values, v)
    end
    return values
end

-- 返回table所有的键值组 { { k1, v1 }, { k2, v2 }, ..., { kn, vn} }
table.items = function(t)
    local items = {}
    for k, v in pairs(t) do
        table.insert(items, { k, v })
    end
    return items
end

-- 返回table的浅拷贝
table.shallowcopy = function(t, needmeta)
    local new = {}
    if needmeta then
        setmetatable(new, getmetatable(t))
    end
    for k, v in pairs(t) do
        new[k] = v
    end
    return new
end

-- 返回table的深拷贝
table.deepcopy = function(t, needmeta)
    local new = {}
    if needmeta then
        setmetatable(new, getmetatable(t))
    end
    for k, v in pairs(t) do
        if type(k) == "table" then
            k = table.deepcopy(k, needmeta)
        end
        if type(v) == "table" then
            v = table.deepcopy(v, needmeta)
        end
        new[k] = v
    end
    return new
end

-- 合并一个table到另一个table上
table.merge = function(dest, src)
    for k, v in pairs(src) do
        dest[k] = v
    end
    return dest
end

-- 判断value是否在table中
table.hasValue = function(t, value)
    for _, v in pairs(t) do
        if v == value then
            return true
        end
    end
    return false
end

-- 返回value对应的key
table.indexValue = function(t, value)
    for k, v in pairs(t) do
        if v == value then
            return k
        end
    end
    return nil
end

-- table扩展end
-----------------


-----------------
-- string扩展start

-- string支持下标运算
do
    local mt = getmetatable("")
    local _index = mt.__index

    mt.__index = function(s, key)
        local integerKey = math.tointeger(key)
        if integerKey then
            return _index.sub(s, integerKey, integerKey)
        else
            return _index[key]
        end
    end
end

string.split = function(s, delimiter, count)
    if type(delimiter) ~= "string" then
        delimiter = "%s"
    end
    local splits = {}
    local pattern = "[^" .. delimiter .. "]+"
    string.gsub(s, pattern, function(v) table.insert(splits, v) end, count)
    return splits
end

string.ltrim = function(s, trimChar)
    if type(trimChar) ~= "string" then
        trimChar = "%s"
    end
    local pattern = "^[" .. trimChar .. "]+"
    return (string.gsub(s, pattern, ""))
end

string.rtrim = function(s, trimChar)
    if type(trimChar) ~= "string" then
        trimChar = "%s"
    end
    local pattern = "[" .. trimChar .. "]+$"
    return (string.gsub(s, pattern, ""))
end

string.trim = function(s, trimChar)
    return string.rtrim(string.ltrim(s, trimChar), trimChar)
end

string.url_encode = function(s)
    s = string.gsub(s, "([^%w%.%- ])", function(c) return string.format("%%%02X", string.byte(c)) end)
    return string.gsub(s, " ", "+")
end

string.url_decode = function(s)
    s = string.gsub(s, '%%(%x%x)', function(h) return string.char(tonumber(h, 16)) end)
    return s
end

string.utf8ByteLength = function(s, i)
    -- argument defaults
    i = i or 1
    -- argument checking
    if type(s) ~= "string" then
        error("bad argument #1 to 'utf8charbytes' (string expected, got ".. type(s).. ")")
    end
    if type(i) ~= "number" then
        error("bad argument #2 to 'utf8charbytes' (number expected, got ".. type(i).. ")")
    end
    local c = s:byte(i)
    -- determine bytes needed for character, based on RFC 3629
    -- validate byte 1
    if c > 0 and c <= 127 then
        -- UTF8-1
        return 1
    elseif c >= 194 and c <= 223 then
        -- UTF8-2
        local c2 = s:byte(i + 1)
        if not c2 then
            return -1
        end
        -- validate byte 2
        if c2 < 128 or c2 > 191 then
            return -1
        end
        return 2
    elseif c >= 224 and c <= 239 then
        -- UTF8-3
        local c2 = s:byte(i + 1)
        local c3 = s:byte(i + 2)
        if not c2 or not c3 then
            return -1
        end
        -- validate byte 2
        if c == 224 and (c2 < 160 or c2 > 191) then
            return -1
        elseif c == 237 and (c2 < 128 or c2 > 159) then
            return -1
        elseif c2 < 128 or c2 > 191 then
            return -1
        end
        -- validate byte 3
        if c3 < 128 or c3 > 191 then
            return -1
        end
        return 3
    elseif c >= 240 and c <= 244 then
        -- UTF8-4
        local c2 = s:byte(i + 1)
        local c3 = s:byte(i + 2)
        local c4 = s:byte(i + 3)
        if not c2 or not c3 or not c4 then
            return -1
        end
        -- validate byte 2
        if c == 240 and (c2 < 144 or c2 > 191) then
            return -1
        elseif c == 244 and (c2 < 128 or c2 > 143) then
            return -1
        elseif c2 < 128 or c2 > 191 then
            return -1
        end
        -- validate byte 3
        if c3 < 128 or c3 > 191 then
            return -1
        end
        -- validate byte 4
        if c4 < 128 or c4 > 191 then
            return -1
        end
        return 4
    else
        return -1
    end
end

local function dump(obj)
    local getIndent, quoteStr, wrapKey, wrapVal, dumpObj
    getIndent = function(level)
        return string.rep("\t", level)
    end
    quoteStr = function(str)
        return '"' .. string.gsub(str, '"', '\\"') .. '"'
    end
    wrapKey = function(val)
        if type(val) == "number" then
            return "[" .. val .. "]"
        elseif type(val) == "string" then
            return "[" .. quoteStr(val) .. "]"
        else
            return "[" .. tostring(val) .. "]"
        end
    end
    wrapVal = function(val, level)
        if type(val) == "table" then
            return dumpObj(val, level)
        elseif type(val) == "number" then
            return val
        elseif type(val) == "string" then
            return quoteStr(val)
        else
            return tostring(val)
        end
    end
    dumpObj = function(obj, level)
        if level >= 100 then
            error("luaext:dump, dump level more than 100, maybe circular table")
            return tostring(nil)
        end
        if type(obj) ~= "table" then
            return wrapVal(obj)
        else
            if type(obj.tostring) == "function" then
                return wrapVal(obj:tostring())
            end
        end
        level = level + 1
        local tokens = {}
        tokens[#tokens + 1] = "{"
        for k, v in pairs(obj) do
            -- 内部解析保留关键字，遍历到说明是pb无数据表
            if k == "_CObj" or k == "_CType" then
                break
            end
            tokens[#tokens + 1] = getIndent(level) .. wrapKey(k) .. " = " .. wrapVal(v, level) .. ","
        end
        tokens[#tokens + 1] = getIndent(level - 1) .. "}"
        return table.concat(tokens, "\n")
    end
    return dumpObj(obj, 0)
end

do
    local _tostring = tostring
    tostring = function(v)
        if type(v) == 'table' then
            return dump(v)
        else
            return _tostring(v)
        end
    end
end

-- string扩展end
-----------------


-----------------
-- math扩展start

do
    local _floor = math.floor
    math.floor = function(n, p)
        if p and p ~= 0 then
            local e = 10 ^ p
            return _floor(n * e) / e
        else
            return _floor(n)
        end
    end
end

math.round = function(n, p)
    local e = 10 ^ (p or 0)
    if not p then
        return math.floor(math.floor(n * e + 0.5) / e)
    else
        return math.floor(n * e + 0.5) / e
    end
end

math.clamp = function (num, min, max)
    if num < min then
        num = min
    elseif num > max then
        num = max
    end
    return num
end

-- math扩展end
-----------------


-----------------
-- math扩展start

local _class = {}

function class(super)
    --对于同一个类型创建的不同obj，需要一种方法判定其属于同一个类
    --同时又不能轻易返回obj真实的metatable
    --这里使用一个不可操作的mt屏蔽obj的真实metatable
    local obj_readonly_mt = setmetatable({}, {
        __newindex = function() end,
        __index = function() end,
        __metatable = "readonly"
    })

    local class_type = {}
    class_type.ctor = false
    class_type.super = super
    class_type.new = function( ... )
        local obj = setmetatable({}, {
            __index = _class[class_type],
            __tostring = _class[class_type].tostring,
            __metatable = obj_readonly_mt
        })
        do
            local create
            create = function(c, ...)
                if c.super then
                    create(c.super, ...)
                end
                if c.ctor then
                    c.ctor(obj, ...)
                end
            end

            create(class_type, ...)
        end
        return obj
    end

    local vtbl = {}
    _class[class_type] = vtbl

    setmetatable(class_type, {
        __newindex = function(t, k, v)
            vtbl[k] = v
        end,
        __index = function(t, k)
            return vtbl[k]
        end,
        __metatable = "readonly"
    })

    if super then
        setmetatable(vtbl, {
            __index = function(t, k)
                local ret = _class[super][k]
                vtbl[k] = ret
                return ret
            end,
            __metatable = "readonly"
        })
    end

    return class_type
end

-- math扩展end
-----------------
