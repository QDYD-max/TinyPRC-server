/// 使用 syslog 作为日志后端，可替代默认的 logger.
//
// 配置
//
//     logservice = "ninjalog"
//     logger = "skynet.server.20170924-a577f12.ci.1"
//
// logger 配置 ident 和 syslog 选项。Ident 必须是有效的文件名，不能包含 /[] 等特殊字符。
//
// 比如可以按照下面方案
//
//     PROJECT.APP.VERSION.ENV.IDENTIFIER
//
// 各字段说明如下：
//
// - PROJECT: 项目名.
// - APP: 应用名，比如 web, worker, server, pusher.
// - VERSION: 版本，可以用 git commit sha1.
// - ENV: 环境，比如 ci, staging
// - IDENTIFIER: 用来区分相同环境中同一个应用的不同实例，比如服务器器 ID.
//
// 如果想要配置 syslog 的参数，可以在 logger 后面添加选项，用逗号分隔，比如
//
//     logger = "skynet.server.20170924-a577f12.ci.1,PERROR,NDELAY,LOCAL6,DEBUG"
//
// 选项的名字即为 `man 3 syslog` 中的选项名字去掉 `LOG_` 前缀。
// 所有日志都使用一个 syslog 的日志等级，需要分级可以使用 JSON 格式日志。
// Facility 只支持 USER 和 LOCAL0~7。
//
// 比较常用的是 `PERROR`，会同时输出日志到 stderr
//
// @module cservice.ninjalog
#include "skynet.h"

#include <syslog.h>
#include <stdio.h>
#include <string.h>

struct ninjalog_config {
	int priority;
	char* programname;
};

struct ninjalog_config *
ninjalog_create(void) {
	struct ninjalog_config* config = skynet_malloc(sizeof(*config));
	config->priority = LOG_INFO;
	config->programname = NULL;
	return config;
}

void
ninjalog_release(struct ninjalog_config *config) {
	closelog();
	if (config->programname != NULL) {
		skynet_free(config->programname);
	}
	skynet_free(config);
}

static int
ninjalog_cb(struct skynet_context * context, void *ud, int type, int session, uint32_t source, const void * msg, size_t sz) {
	struct ninjalog_config *config = ud;

	switch (type) {
		case PTYPE_TEXT:{
			const char* f = (char*)msg;
			const char ch = '\"';
			if (strchr(f, ch) == f) {
				syslog(config->priority, "{\"address\":\"%08x\",%s}", source, (const char *)msg);
			}
			else {
				//skynet 自带 error log 格式包装成 info
				syslog(config->priority, "{\"address\":\"%08x\",\"level\":\"INFO\",\"service\":\"default\",\"msg\":\"%s\"}", source, (const char *)msg);
			}
			break;
		}
	}

	return 0;
}

#define NINJA_SYSLOG_MAX_PRIORITY_POS 7
#define NINJA_SYSLOG_MAX_OPEN_OPTIONS_POS 11

int
ninjalog_init(struct ninjalog_config *config, struct skynet_context *ctx, const char * parm) {
	static const int ioptions[] = {
			LOG_EMERG,
			LOG_ALERT,
			LOG_CRIT,
			LOG_ERR,
			LOG_WARNING,
			LOG_NOTICE,
			LOG_INFO,
			LOG_DEBUG, // 7

			LOG_CONS,
			LOG_NDELAY,
			LOG_PERROR,
			LOG_PID, // 11

			LOG_USER,
			LOG_LOCAL0,
			LOG_LOCAL1,
			LOG_LOCAL2,
			LOG_LOCAL3,
			LOG_LOCAL4,
			LOG_LOCAL5,
			LOG_LOCAL6,
			LOG_LOCAL7
	};

	static const char * coptions[] = {
			"EMERG",
			"ALERT",
			"CRIT",
			"ERR",
			"WARNING",
			"NOTICE",
			"INFO",
			"DEBUG",

			"CONS",
			"NDELAY",
			"PERROR",
			"PID",

			"USER",
			"LOCAL0",
			"LOCAL1",
			"LOCAL2",
			"LOCAL3",
			"LOCAL4",
			"LOCAL5",
			"LOCAL6",
			"LOCAL7",

			NULL
	};

	static const char * const sep = ",";
	static int default_options = LOG_NDELAY | LOG_PID;
	static int default_facility = LOG_LOCAL6;

	if (parm == NULL) {
		openlog("skynet", default_options, default_facility);
	} else {
		size_t len = strlen(parm);
		char* programname = skynet_malloc(sizeof(char) * (len + 1));
		memcpy(programname, parm, len + 1);

		char *next = programname;
		char *opt;
		const char **current_opt;
		int current_pos;
		int options = 0;
		int facility = default_facility;

		// first must be the logger
		strsep(&next, sep);

		if (next) {
			for (opt = strsep(&next, sep); opt; opt = strsep(&next, sep)) {
				current_pos = 0;
				current_opt = &coptions[0];
				while (*current_opt && 0 != strcmp(opt, *current_opt)) {
					current_pos += 1;
					current_opt += 1;
				}
				if (*current_opt) {
					if (current_pos <= NINJA_SYSLOG_MAX_PRIORITY_POS) {
						config->priority = ioptions[current_pos];
					} else if (current_pos <= NINJA_SYSLOG_MAX_OPEN_OPTIONS_POS) {
						options |= ioptions[current_pos];
					} else {
						facility = ioptions[current_pos];
					}
				}
			}
		}

		if (options == 0) {
			options = default_options;
		}

		openlog(programname, options, facility);
		// Logger may quit before finish initialization,
		// must set to config after parsing.
		config->programname = programname;
	}
	skynet_callback(ctx, config, ninjalog_cb);
	skynet_command(ctx, "REG", ".logger");
	return 0;
}
