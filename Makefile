TARGET=		lsm_tree

REBAR=		/usr/bin/env rebar
ERL=		/usr/bin/env erl
DIALYZER=	dialyzer

.PHONY: plt analyze all deps compile get-deps clean

all: compile

deps: get-deps

get-deps:
	@$(REBAR) get-deps

compile:
	@$(REBAR) compile

clean:
	@$(REBAR) clean

test: eunit

eunit: compile clean-test-btrees
	@$(REBAR) eunit skip_deps=true

eunit_console:
	@$(ERL) -pa .eunit deps/*/ebin

# TODO fix this
clean-test-btrees:
	@rm -fr .eunit/Btree_* .eunit/simple

plt: compile
	@$(DIALYZER) --build_plt --output_plt .$(TARGET).plt \
		-pa deps/plain_fsm/ebin \
		deps/plain_fsm/ebin \
		--apps kernel stdlib

analyze: compile
	$(DIALYZER) --plt .$(TARGET).plt \
	-pa deps/plain_fsm/ebin \
	-pa deps/ebloom/ebin \
	ebin

repl:
	$(ERL) -pz deps/*/ebin -pa ebin
