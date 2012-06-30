
REBAR=		/usr/bin/env rebar

all: deps
	$(REBAR) compile

clean:
	$(REBAR) clean

deps:
	$(REBAR) get-deps

test:
	$(REBAR) test
