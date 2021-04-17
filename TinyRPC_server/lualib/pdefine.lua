local skynet = require "skynet"

PDEFINE = {}

PDEFINE.MONGO_COLLECTION = {
    ["CLUSTER"]     = "clusters",
    ["ACCOUNT"]     = "accounts",
    ["GAME_AGENT"]  = "game_agents"
}

-- 时间
PDEFINE.SECONDS = {
    ["SECOND"]  = 1,
    ["MINUTE"]  = 60,
    ["HOUR"]    = 3600,
    ["DAY"]     = 1   * 24 * 3600,
    ["WEEK"]    = 7   * 24 * 3600,
    ["MONTH"]   = 30  * 24 * 3600,
    ["YEAR"]    = 365 * 24 * 3600
}

-- 错误码
PDEFINE.RET = {
    ["SUCCESS"]     = 200, -- 成功
    ["UNDEFINE"]    = 300, -- 未定义错误
    -- 登录期间错误码
    ["LOGIN_ERROR"] = {
        ["HMAC_ERROR"]          = 301, -- 加密校验错误(login,node)
        ["CALL_ERROR"]          = 302, -- 调用错误(login,node)
        ["PARAMS_ERROR"]        = 303, -- 参数错误(login,node)
        ["SOCKET_ERROR"]        = 304, -- socket错误(login,node)
        ["WHITE_IP_ERROR"]      = 305, -- 不在IP白名单里面(login)
        ["PACKAGE_SIGN_ERROR"]  = 306, -- 包体签名错误(login)
        ["DEVICE_BE_BANNED"]    = 307, -- 设备被禁(login)
        ["LOGIN_UNION_CLOSE"]   = 308, -- 安卓和苹果联合登录关闭(login)
        ["NEED_PHONE"]          = 309, -- 需要通过手机登录(login)
        ["CREATE_FORBIDDEN"]    = 310, -- 建号冷冻期(login)
        ["NEED_SMSCODE"]        = 311, -- 需要提供登录验证码(login)
        ["SMSCODE_ERROR"]       = 312, -- 登录验证码错误(login)
        ["FASTPHONE_ERROR"]     = 313, -- 闪验账号错误(login)
        ["ACCOUNT_ERROR"]       = 314, -- 登录账号错误(login)
        ["WHITE_ACCOUNT_ERROR"] = 315, -- 不在账号白名单里面(login)
        ["TOKEN_ERROR"]         = 316, -- 重登票据失效(login)
        ["REGISTER_ERROR"]      = 317, -- 注册账号错误(login)
        ["LOGIN_FORBIDDEN"]     = 318, -- 已经封停(login)
        ["ALREADY_LOGIN"]       = 319, -- 已经登录(login)
        ["UNAUTHORIZED"]        = 320, -- 认证失败(node)
        ["INDEX_EXPIRED"]       = 321, -- 重连索引过期(node)
        ["NODE_BUSY"]           = 322, -- 登录目标node繁忙(login)
        ["NOTIFY_EXPIRED"]      = 323, -- 重连时notify ack过期
    },
    -- 游戏内错误码
    ["ERROR"] = {
        ["CALL_ERROR"]          = 400, -- 调用错误
        ["DECODE_FAIL"]         = 401, -- 解析protocbuf错误
        ["PACKAGE_TOO_LARGE"]   = 402, -- 回包太大
        ["ERRORTEST"]           = 999, -- 测试用例
    },
    ["PLAYER_ERROR"] = {
        ["CREATE_FAILED"]       = 1000, -- player创建失败
        ["PLAYER_EXISTS"]       = 1001, -- player已经存在
    }
}

-- 登录账号渠道
PDEFINE.LOGIN_CHANNEL = {
    ["OFFICIAL"]    = "OFFICIAL",
    ["QQ"]          = 2,
    ["WECHAT"]      = 3,
    ["BILIBILI"]    = 4,
    ["XIAOMI"]      = 5,
    ["OPPO"]        = 6,
    ["VIVO"]        = 7,
    ["HUAWEI"]      = 8,
    ["MEIZU"]       = 9,
    ["LENOVO"]      = 10,
    ["GIONEE"]      = 11,
    ["COOLPAD"]     = 12,
    ["M4399"]       = 16,
    ["JIUYOU"]      = 18,
    ["APPLE"]       = 1000,
}