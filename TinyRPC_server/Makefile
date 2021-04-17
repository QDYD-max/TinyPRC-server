.PHONY : all skynet clean

PLAT ?= linux
SKYNET ?= skynet
LUA_INC ?= $(SKYNET)/3rd/lua
CFLAGS := -g -O2 -Wall -I$(LUA_INC) -I$(SKYNET)/skynet-src $(MYCFLAGS)
SHARED := -fPIC --shared $(MYSHARED)

CSERVICE_PATH ?= cservice
CSERVICE := ninjalog

LUA_CLIB_PATH ?= luaclib
LUA_CLIB := protobuf snowflake

PROTO := $(patsubst %.proto, %.pb, $(wildcard proto/*.proto))


all : skynet

skynet : $(SKYNET)/Makefile
	cd $(SKYNET) && $(MAKE) CC=$(CC) $(PLAT)

$(SKYNET)/Makefile :
	git submodule update --init


all : $(foreach v, $(CSERVICE), $(CSERVICE_PATH)/$(v).so)

define CSERVICE_TEMP
  $$(CSERVICE_PATH)/$(1).so : service-src/service_$(1).c | $$(CSERVICE_PATH)
	$$(CC) $$(CFLAGS) $$(SHARED) $$< -o $$@
endef
$(foreach v, $(CSERVICE), $(eval $(call CSERVICE_TEMP,$(v))))

$(CSERVICE_PATH) :
	mkdir $(CSERVICE_PATH)


all : $(foreach v, $(LUA_CLIB), $(LUA_CLIB_PATH)/$(v).so)

$(LUA_CLIB_PATH)/protobuf.so : $(wildcard lualib-src/pbc/*) | $(LUA_CLIB_PATH)
	$(CC) $(CFLAGS) $(SHARED) -Ilualib-src/pbc $^ -o $@

$(LUA_CLIB_PATH)/snowflake.so : lualib-src/lua-snowflake.c | $(LUA_CLIB_PATH)
	$(CC) $(CFLAGS) $(SHARED) $^ -o $@

$(LUA_CLIB_PATH) :
	mkdir $(LUA_CLIB_PATH)


all : $(PROTO)

$(PROTO) : %.pb : %.proto
	protoc $< -o $@


clean :
	cd $(SKYNET) && $(MAKE) clean
	rm -f $(CSERVICE_PATH)/*.so $(LUA_CLIB_PATH)/*.so $(PROTO)
