
all: deps
	rebar compile

clean:
	rebar clean

deps:
	rebar get-deps

test:
	rebar test
