local date = {}

--region weekday and wday
-- 正常认知的周几转换为Lua中的wday
function date.weekday_to_wday(weekday)
    assert(weekday >= 1, "weekday is 1,2,3,4,5,6,7")
    assert(weekday <= 7, "weekday is 1,2,3,4,5,6,7")

    local wday = weekday + 1
    if wday > 7 then
        wday = 1
    end
    return wday
end

-- Lua中的way转换为正常认知的周几
function date.wday_to_weekday(wday)
    assert(wday >= 1, "wday is 2,3,4,5,6,7,1")
    assert(wday <= 7, "wday is 2,3,4,5,6,7,1")

    local weekday = wday - 1
    if weekday < 1 then
        weekday = 7
    end
    return weekday
end
--endregion

--region get zero
-- 获得 0 秒的时间戳
-- refTime默认为当前
-- offset以分为单位进行偏移，默认为0
function date.get_min_zero_time(refTime, offset)
    refTime = math.tointeger(refTime) or os.time()
    offset = math.tointeger(offset) or 0

    local refDate = os.date("*t", refTime)
    return os.time({ year = refDate.year, month = refDate.month, day = refDate.day, hour = refDate.hour, min = refDate.min+offset, sec = 0 })
end

-- 获得 0 分 0 秒的时间戳
-- refTime默认为当前
-- offset以时为单位进行偏移，默认为0
function date.get_hour_zero_time(refTime, offset)
    refTime = math.tointeger(refTime) or os.time()
    offset = math.tointeger(offset) or 0

    local refDate = os.date("*t", refTime)
    return os.time({ year = refDate.year, month = refDate.month, day = refDate.day, hour = refDate.hour+offset, min = 0, sec = 0 })
end

-- 获得 0 时 0 分 0 秒的时间戳
-- refTime默认为当前
-- offset以天为单位进行偏移，默认为0
function date.get_day_zero_time(refTime, offset)
    refTime = math.tointeger(refTime) or os.time()
    offset = math.tointeger(offset) or 0

    local refDate = os.date("*t", refTime)
    return os.time({ year = refDate.year, month = refDate.month, day = refDate.day+offset, hour = 0, min = 0, sec = 0 })
end

-- 获得 周一 0 时 0 分 0 秒的时间戳
-- refTime默认为当前
-- offset以周为单位进行偏移，默认为0
function date.get_week_zero_time(refTime, offset)
    refTime = math.tointeger(refTime) or os.time()
    offset = math.tointeger(offset) or 0

    local refDate = os.date("*t", refTime)
    local refWeekday = date.wday_to_weekday(refDate.wday)
    return os.time({ year = refDate.year, month = refDate.month, day = refDate.day-(refWeekday-1)+offset*7, hour = 0, min = 0, sec = 0 })
end

-- 获得 1 号 0 时 0 分 0 秒的时间戳
-- refTime默认为当前
-- offset以月为单位进行偏移，默认为0
function date.get_month_zero_time(refTime, offset)
    refTime = math.tointeger(refTime) or os.time()
    offset = math.tointeger(offset) or 0

    local refDate = os.date("*t", refTime)
    return os.time({ year = refDate.year, month = refDate.month+offset, day = 1, hour = 0, min = 0, sec = 0 })
end

-- 获得 1 月 1 号 0 时 0 分 0 秒的时间戳
-- refTime默认为当前
-- offset以年为单位进行偏移，默认为0
function date.get_year_zero_time(refTime, offset)
    refTime = math.tointeger(refTime) or os.time()
    offset = math.tointeger(offset) or 0

    local refDate = os.date("*t", refTime)
    return os.time({ year = refDate.year+offset, month = 1, day = 1, hour = 0, min = 0, sec = 0 })
end
--endregion

--region day cycle
-- 获得24小时内给定时刻的下一个时间点的时间戳
-- refTime默认为当前
-- hour，min，sec默认均为0
function date.get_next_day_time(refTime, hour, min, sec)
    refTime = math.tointeger(refTime) or os.time()
    hour = math.tointeger(hour) or 0
    min = math.tointeger(min) or 0
    sec = math.tointeger(sec) or 0

    local refDate = os.date("*t", refTime)
    local nextDayTime = os.time({ year = refDate.year, month = refDate.month, day = refDate.day, hour = hour, min = min, sec = sec })
    if os.difftime(nextDayTime, refTime) < 0 then
        nextDayTime = os.time({ year = refDate.year, month = refDate.month, day = refDate.day+1, hour = hour, min = min, sec = sec })
    end
    return nextDayTime
end

-- 获得24小时内给定时刻的上一个时间点的时间戳
-- refTime默认为当前
-- hour，min，sec默认均为0
function date.get_last_day_time(refTime, hour, min, sec)
    refTime = math.tointeger(refTime) or os.time()
    hour = math.tointeger(hour) or 0
    min = math.tointeger(min) or 0
    sec = math.tointeger(sec) or 0

    local refDate = os.date("*t", refTime)
    local lastDayTime = os.time({ year = refDate.year, month = refDate.month, day = refDate.day, hour = hour, min = min, sec = sec })
    if os.difftime(lastDayTime, refTime) > 0 then
        lastDayTime = os.time({ year = refDate.year, month = refDate.month, day = refDate.day-1, hour = hour, min = min, sec = sec })
    end
    return lastDayTime
end
--endregion

--region week cycle
-- 获得1周内给定时刻的下一个时间点的时间戳
-- refTime默认为当前
-- weekday默认为1，hour，min，sec默认均为0
function date.get_next_week_time(refTime, weekday, hour, min, sec)
    refTime = math.tointeger(refTime) or os.time()
    weekday = math.tointeger(weekday) or 1
    assert(weekday >= 1, "weekday is 1,2,3,4,5,6,7")
    assert(weekday <= 7, "weekday is 1,2,3,4,5,6,7")
    hour = math.tointeger(hour) or 0
    min = math.tointeger(min) or 0
    sec = math.tointeger(sec) or 0

    local refDate = os.date("*t", refTime)
    local refWeekday = date.wday_to_weekday(refDate.wday)
    local nextWeekTime = os.time({ year = refDate.year, month = refDate.month, day = refDate.day+(weekday-refWeekday), hour = hour, min = min, sec = sec })
    if os.difftime(nextWeekTime, refTime) < 0 then
        nextWeekTime = os.time({ year = refDate.year, month = refDate.month, day = refDate.day+(weekday-refWeekday)+7, hour = hour, min = min, sec = sec })
    end
    return nextWeekTime
end

-- 获得1周内给定时刻的上一个时间点的时间戳
-- refTime默认为当前
-- weekday默认为1，hour，min，sec默认均为0
function date.get_last_week_time(refTime, weekday, hour, min, sec)
    refTime = math.tointeger(refTime) or os.time()
    weekday = math.tointeger(weekday) or 1
    assert(weekday >= 1, "weekday is 1,2,3,4,5,6,7")
    assert(weekday <= 7, "weekday is 1,2,3,4,5,6,7")
    hour = math.tointeger(hour) or 0
    min = math.tointeger(min) or 0
    sec = math.tointeger(sec) or 0

    local refDate = os.date("*t", refTime)
    local refWeekday = date.wday_to_weekday(refDate.wday)
    local lastWeekTime = os.time({ year = refDate.year, month = refDate.month, day = refDate.day+(weekday-refWeekday), hour = hour, min = min, sec = sec })
    if os.difftime(lastWeekTime, refTime) > 0 then
        lastWeekTime = os.time({ year = refDate.year, month = refDate.month, day = refDate.day+(weekday-refWeekday)-7, hour = hour, min = min, sec = sec })
    end
    return lastWeekTime
end
--endregion

--region month cycle
-- 获得1月内给定时刻的下一个时间点的时间戳
-- refTime默认为当前
-- day默认为1，hour，min，sec默认均为0
function date.get_next_month_time(refTime, day, hour, min, sec)
    refTime = math.tointeger(refTime) or os.time()
    day = math.tointeger(day) or 1
    hour = math.tointeger(hour) or 0
    min = math.tointeger(min) or 0
    sec = math.tointeger(sec) or 0

    local refDate = os.date("*t", refTime)
    local nextMonthTime = os.time({ year = refDate.year, month = refDate.month, day = day, hour = hour, min = min, sec = sec })
    if os.difftime(nextMonthTime, refTime) < 0 then
        nextMonthTime = os.time({ year = refDate.year, month = refDate.month+1, day = day, hour = hour, min = min, sec = sec })
    end
    return nextMonthTime
end

-- 获得1月内给定时刻的上一个时间点的时间戳
-- refTime默认为当前
-- day默认为1，hour，min，sec默认均为0
function date.get_last_month_time(refTime, day, hour, min, sec)
    refTime = math.tointeger(refTime) or os.time()
    day = math.tointeger(day) or 1
    hour = math.tointeger(hour) or 0
    min = math.tointeger(min) or 0
    sec = math.tointeger(sec) or 0

    local refDate = os.date("*t", refTime)
    local lastMonthTime = os.time({ year = refDate.year, month = refDate.month, day = day, hour = hour, min = min, sec = sec })
    if os.difftime(lastMonthTime, refTime) > 0 then
        lastMonthTime = os.time({ year = refDate.year, month = refDate.month-1, day = day, hour = hour, min = min, sec = sec })
    end
    return lastMonthTime
end
--endregion

return date