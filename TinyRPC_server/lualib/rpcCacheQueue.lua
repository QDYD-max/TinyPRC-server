
local rpcCacheQueue = {}
rpcCacheQueue.__index = rpcCacheQueue

function rpcCacheQueue.new(size)
    local q = {}
    setmetatable(q, rpcCacheQueue)
    q:set(size)
    return q
end

function rpcCacheQueue:set(size)
    self._idx = 1
    self._maxSize = size or 100
    self._cache = {}
end

function rpcCacheQueue:get_sequence()
    return self._idx
end

function rpcCacheQueue:push(msg)
    -- print("rpcCacheQueue push: ", self._idx)
    self._cache[self._idx % self._maxSize] = msg
    self._idx = self._idx + 1
end

function rpcCacheQueue:flush(begin, callback)
    for i = begin + 1, self._idx - 1 do
        callback(self._cache[i % self._maxSize])
    end
end

function rpcCacheQueue:check_expired(index)
    return not table.empty(self._cache) and index < math.max(self._idx - self._maxSize, 0)
end

function rpcCacheQueue:clear()
    self._cache = {}
    self._idx = 1
end

return rpcCacheQueue

