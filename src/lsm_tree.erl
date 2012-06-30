%% -*- coding: utf-8; Mode: erlang; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*-
%% ex: set softtabstop=4 tabstop=4 shiftwidth=4 expandtab fileencoding=utf-8:

%% ----------------------------------------------------------------------------
%%
%% lsm_tree: A Riak/KV backend using SQLite4's Log-Structured Merge Tree
%%
%% Copyright 2012 (c) Basho Technologies, Inc.  All Rights Reserved.
%% http://basho.com/ info@basho.com
%%
%% This file is provided to you under the Apache License, Version 2.0 (the
%% "License"); you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
%% License for the specific language governing permissions and limitations
%% under the License.
%%
%% ----------------------------------------------------------------------------

-module(lsm_tree).
-author('Greg Burd <greg@burd.me>').

-ifdef(DEBUG).
-define(log(Fmt,Args),io:format(user,Fmt,Args)).
-else.
-define(log(Fmt,Args),ok).
-endif.

-export([  open/2
         , close/1
         , get/2
         , put/3
         , delete/2
         , fold/3
         , fold_range/4
         , fold_keys/3
         , fold_values/3
         , foldl/3
         , foldl_keys/3
         , foldl_values/3
         , destroy/1
         , salvage/1
         , sync/1
         , compact/1
         , truncate/1
         , cursor_open/1
         , cursor_close/1
         , cursor_position/2
         , cursor_next/1
         , cursor_next_key/1
         , cursor_next_value/1
         , cursor_prev/1
         , cursor_prev_key/1
         , cursor_prev_value/1
         , cursor_first/1
         , cursor_last/1
         % TODO is_empty/1, count/1, size/1, txn_begin/2, txn_begin/3, txn_commit/2,
         % txn_abort/2, snapshot/1, stat/2
         ]).

-include("include/lsm_tree.hrl").
-include("plain_rpc.hrl").

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-type config() :: binary().
-type config_list() :: [{atom(), any()}].
-opaque tree() :: reference().
-opaque cursor() :: reference().
-type key() :: binary().
-type value() :: binary().

-export_type([tree/0, cursor/0]).

-on_load(init/0).

-define(nif_stub, nif_stub_error(?LINE)).
nif_stub_error(Line) ->
    erlang:nif_error({nif_not_loaded,module,?MODULE,line,Line}).

-spec init() -> ok | {error, any()}.
init() ->
    PrivDir = case code:priv_dir(?MODULE) of
                  {error, bad_name} ->
                      EbinDir = filename:dirname(code:which(?MODULE)),
                      AppPath = filename:dirname(EbinDir),
                      filename:join(AppPath, "priv");
                  Path ->
                      Path
              end,
    erlang:load_nif(filename:join(PrivDir, atom_to_list(?MODULE)), 0).

-spec open(string(), open_options()) -> {ok, tree()} | {error, term()}.
open(_Path, _Options) ->
    ?nif_stub.

-spec close(tree()) -> ok | {error, term()}.
close(_ConnRef) ->
    ?nif_stub.

-spec get(tree(), key()) -> {ok, value()} | not_found | {error, term()}.
get(_Ref, _Key) ->
    ?nif_stub.

-spec put(tree(), key(), value()) -> ok | {error, term()}.
put(_Ref, _Key, _Value) ->
    ?nif_stub.

-spec delete(tree(), key()) -> ok | {error, term()}.
delete(_Ref, _Key) ->
    ?nif_stub.

-spec truncate(tree()) -> ok | {error, term()}.
truncate(Ref) ->
    ?nif_stub.

-spec verify(tree()) -> ok | {error, term()}.
verify(_Ref) ->
    ?nif_stub.

-spec salvage(string()) -> ok | {error, term()}.
salvage(_Path) ->
    ?nif_stub.

-spec sync(tree()) -> ok | {error, term()}.
sync(_Ref) ->
    ?nif_stub.

-spec compact(tree()) -> ok | {error, term()}.
compact(_Ref) ->
    ?nif_stub.

-spec destroy(tree()) -> ok | {error, term()}.
destroy(_Ref) ->
    ?nif_stub.

-spec upgrade(tree()) -> ok | {error, term()}.
upgrade(_Ref) ->
    throw(not_yet_implemented). % TODO: ?nif_stub.

-spec cursor_open(tree()) -> {ok, cursor()} | {error, term()}.
cursor_open(_Ref) ->
    ?nif_stub.

-spec cursor_close(cursor()) -> ok | {error, term()}.
cursor_close(_Cursor) ->
    ?nif_stub.

-spec cursor_position(cursor(), key()) -> {ok, value()} | {error, term()}.
cursor_position(_Cursor, _Key) ->
    ?nif_stub.

-spec cursor_next(cursor()) -> {ok, key(), value()} | not_found | {error, term()}.
cursor_next(_Cursor) ->
    ?nif_stub.

-spec cursor_next_key(cursor()) -> {ok, key()} | not_found | {error, term()}.
cursor_next_key(_Cursor) ->
    ?nif_stub.

-spec cursor_next_value(cursor()) -> {ok, value()} | not_found | {error, term()}.
cursor_next_value(_Cursor) ->
    ?nif_stub.

-spec cursor_prev(cursor()) -> {ok, key(), value()} | not_found | {error, term()}.
cursor_prev(_Cursor) ->
    ?nif_stub.

-spec cursor_prev_key(cursor()) -> {ok, key()} | not_found | {error, term()}.
cursor_prev_key(_Cursor) ->
    ?nif_stub.

-spec cursor_prev_value(cursor()) -> {ok, value()} | not_found | {error, term()}.
cursor_prev_value(_Cursor) ->
    ?nif_stub.

-spec cursor_first(cursor()) -> ok | {error, term()}.
cursor_first(_Cursor) ->
    ?nif_stub.

-spec cursor_last(cursor()) -> ok | {error, term()}.
cursor_last(_Cursor) ->
    ?nif_stub.

-type fold_keys_fun() :: fun((Key::binary(), any()) -> any()).

-spec fold_keys(cursor(), fold_keys_fun(), any()) -> any().
fold_keys(Cursor, Fun, Acc0) ->
    fold_keys(Cursor, Fun, Acc0, cursor_next_key(Cursor)).
fold_keys(_Cursor, _Fun, Acc, not_found) ->
    Acc;
fold_keys(Cursor, Fun, Acc, {ok, Key}) ->
    fold_keys(Cursor, Fun, Fun(Key, Acc), cursor_next_key(Cursor)).

-spec foldl_keys(cursor(), fold_keys_fun(), any()) -> any().
foldl_keys(Cursor, Fun, Acc0) ->
    foldl_keys(Cursor, Fun, Acc0, cursor_prev_key(Cursor)).
foldl_keys(_Cursor, _Fun, Acc, not_found) ->
    Acc;
foldl_keys(Cursor, Fun, Acc, {ok, Key}) ->
    foldl_keys(Cursor, Fun, Fun(Key, Acc), cursor_prev_key(Cursor)).

-type fold_values_fun() :: fun((Key::binary(), any()) -> any()).

-spec fold_values(cursor(), fold_keys_fun(), any()) -> any().
fold_values(Cursor, Fun, Acc0) ->
    fold_values(Cursor, Fun, Acc0, cursor_next_value(Cursor)).
fold_values(_Cursor, _Fun, Acc, not_found) ->
    Acc;
fold_values(Cursor, Fun, Acc, {ok, Key}) ->
    fold_values(Cursor, Fun, Fun(Key, Acc), cursor_next_value(Cursor)).

-spec foldl_values(cursor(), fold_keys_fun(), any()) -> any().
foldl_values(Cursor, Fun, Acc0) ->
    foldl_values(Cursor, Fun, Acc0, cursor_prev_value(Cursor)).
foldl_values(_Cursor, _Fun, Acc, not_found) ->
    Acc;
foldl_values(Cursor, Fun, Acc, {ok, Key}) ->
    foldl_values(Cursor, Fun, Fun(Key, Acc), cursor_prev_value(Cursor)).

-type fold_fun() :: fun(({Key::binary(), Value::binary()}, any()) -> any()).

-spec fold(cursor(), fold_fun(), any()) -> any().
fold(Cursor, Fun, Acc0) ->
    fold(Cursor, Fun, Acc0, cursor_next(Cursor)).
fold(_Cursor, _Fun, Acc, not_found) ->
    Acc;
fold(Cursor, Fun, Acc, {ok, Key, Value}) ->
    fold(Cursor, Fun, Fun({Key, Value}, Acc), cursor_next(Cursor)).

-spec foldl(cursor(), fold_fun(), any()) -> any().
foldl(Cursor, Fun, Acc0) ->
    foldl(Cursor, Fun, Acc0, cursor_prev(Cursor)).
foldl(_Cursor, _Fun, Acc, not_found) ->
    Acc;
foldl(Cursor, Fun, Acc, {ok, Key, Value}) ->
    foldl(Cursor, Fun, Fun({Key, Value}, Acc), cursor_prev(Cursor)).

-spec fold_range(tree(), fold_fun(), any(), key_range()) -> any().
fold_range(Ref, Fun, Acc0, Range) ->
    {ok, FoldWorkerPID} = lsm_tree_fold_worker:start(self()),
    Method =
        case Range#key_range.limit < 10 of
            true -> blocking_range;
            false -> snapshot_range
        end,
    ok = gen_server:call(Ref, {Method, FoldWorkerPID, Range}, infinity),
    MRef = erlang:monitor(process, FoldWorkerPID),
    ?log("fold_range begin: self=~p, worker=~p~n", [self(), FoldWorkerPID]),
    Result = receive_fold_range(MRef, FoldWorkerPID, Fun, Acc0, Range#key_range.limit),
    ?log("fold_range done: self:~p, result=~P~n", [self(), Result, 20]),
    Result.

receive_fold_range(MRef, PID, _, Acc0, 0) ->
    erlang:exit(PID, shutdown),
    drain_worker_and_return(MRef, PID, Acc0);

receive_fold_range(MRef, PID, Fun, Acc0, Limit) ->
    ?log("receive_fold_range:~p,~P~n", [PID,Acc0,10]),
    receive
        %% receive one K/V from fold_worker
        ?CALL(From, {fold_result, PID, K,V}) ->
            plain_rpc:send_reply(From, ok),
            case
                try
                    {ok, Fun(K,V,Acc0)}
                catch
                    Class:Exception ->
                        % ?log("Exception in lsm_tree fold: ~p ~p", [Exception, erlang:get_stacktrace()]),
                        % lager:warn("Exception in lsm_tree fold: ~p", [Exception]),
                        {'EXIT', Class, Exception, erlang:get_stacktrace()}
                end
            of
                {ok, Acc1} ->
                    receive_fold_range(MRef, PID, Fun, Acc1, decr(Limit));
                Exit ->
                    %% kill the fold worker ...
                    erlang:exit(PID, shutdown),
                    drain_worker_and_throw(MRef, PID, Exit)
            end;

        ?CAST(_,{fold_limit, PID, _}) ->
            ?log("> fold_limit pid=~p, self=~p~n", [PID, self()]),
            erlang:demonitor(MRef, [flush]),
            Acc0;
        ?CAST(_,{fold_done, PID}) ->
            ?log("> fold_done pid=~p, self=~p~n", [PID, self()]),
            erlang:demonitor(MRef, [flush]),
            Acc0;
        {'DOWN', MRef, _, _PID, normal} ->
            ?log("> fold worker ~p ENDED~n", [_PID]),
            Acc0;
        {'DOWN', MRef, _, _PID, Reason} ->
            ?log("> fold worker ~p DOWN reason:~p~n", [_PID, Reason]),
            error({fold_worker_died, Reason})
    end.

decr(undefined) ->
    undefined;
decr(N) ->
    N-1.

%%
%% Just calls erlang:raise with appropriate arguments
%%
raise({'EXIT', Class, Exception, Trace}) ->
    erlang:raise(Class, Exception, Trace).

%%
%% When an exception has happened in the fold function, we use
%% this to drain messages coming from the fold_worker before
%% re-throwing the exception.
%%
drain_worker_and_throw(MRef, PID, ExitTuple) ->
    receive
        ?CALL(_From,{fold_result, PID, _, _}) ->
            drain_worker_and_throw(MRef, PID, ExitTuple);
        {'DOWN', MRef, _, _, _} ->
            raise(ExitTuple);
        ?CAST(_,{fold_limit, PID, _}) ->
            erlang:demonitor(MRef, [flush]),
            raise(ExitTuple);
        ?CAST(_,{fold_done, PID}) ->
            erlang:demonitor(MRef, [flush]),
            raise(ExitTuple)
    after 0 ->
            raise(ExitTuple)
    end.

drain_worker_and_return(MRef, PID, Value) ->
    receive
        ?CALL(_From,{fold_result, PID, _, _}) ->
            drain_worker_and_return(MRef, PID, Value);
        {'DOWN', MRef, _, _, _} ->
            Value;
        ?CAST(_,{fold_limit, PID, _}) ->
            erlang:demonitor(MRef, [flush]),
            Value;
        ?CAST(_,{fold_done, PID}) ->
            erlang:demonitor(MRef, [flush]),
            Value
    after 0 ->
            Value
    end.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-define(TEST_DATA_DIR, "test/lsm_tree.basic").

open_test_conn(DataDir) ->
    ?assertCmd("rm -rf "++DataDir),
    ?assertMatch(ok, filelib:ensure_dir(filename:join(DataDir, "x"))),
    OpenConfig = config_to_bin([{create,true},{cache_size,"100MB"}]),
    {ok, ConnRef} = conn_open(DataDir, OpenConfig),
    ConnRef.

open_test_session(ConnRef) ->
    {ok, SRef} = session_open(ConnRef),
    ?assertMatch(ok, session_drop(SRef, "table:test", config_to_bin([{force,true}]))),
    ?assertMatch(ok, session_create(SRef, "table:test")),
    SRef.

conn_test() ->
    ConnRef = open_test_conn(?TEST_DATA_DIR),
    ?assertMatch(ok, conn_close(ConnRef)).

session_test_() ->
    {setup,
     fun() ->
             open_test_conn(?TEST_DATA_DIR)
     end,
     fun(ConnRef) ->
             ok = conn_close(ConnRef)
     end,
     fun(ConnRef) ->
             {inorder,
              [{"open/close a session",
                fun() ->
                        {ok, SRef} = session_open(ConnRef),
                        ?assertMatch(ok, session_close(SRef))
                end},
               {"create and drop a table",
                fun() ->
                        SRef = open_test_session(ConnRef),
                        ?assertMatch(ok, session_drop(SRef, "table:test")),
                        ?assertMatch(ok, session_close(SRef))
                end}]}
     end}.

insert_delete_test() ->
    ConnRef = open_test_conn(?TEST_DATA_DIR),
    SRef = open_test_session(ConnRef),
    ?assertMatch(ok, session_put(SRef, "table:test", <<"a">>, <<"apple">>)),
    ?assertMatch({ok, <<"apple">>}, session_get(SRef, "table:test", <<"a">>)),
    ?assertMatch(ok,  session_delete(SRef, "table:test", <<"a">>)),
    ?assertMatch(not_found,  session_get(SRef, "table:test", <<"a">>)),
    ok = session_close(SRef),
    ok = conn_close(ConnRef).

init_test_table() ->
    ConnRef = open_test_conn(?TEST_DATA_DIR),
    SRef = open_test_session(ConnRef),
    ?assertMatch(ok, session_put(SRef, "table:test", <<"a">>, <<"apple">>)),
    ?assertMatch(ok, session_put(SRef, "table:test", <<"b">>, <<"banana">>)),
    ?assertMatch(ok, session_put(SRef, "table:test", <<"c">>, <<"cherry">>)),
    ?assertMatch(ok, session_put(SRef, "table:test", <<"d">>, <<"date">>)),
    ?assertMatch(ok, session_put(SRef, "table:test", <<"g">>, <<"gooseberry">>)),
    {ConnRef, SRef}.

stop_test_table({ConnRef, SRef}) ->
    ?assertMatch(ok, session_close(SRef)),
    ?assertMatch(ok, conn_close(ConnRef)).

various_session_test_() ->
    {setup,
     fun init_test_table/0,
     fun stop_test_table/1,
     fun({_, SRef}) ->
             {inorder,
              [{"session verify",
                fun() ->
                        ?assertMatch(ok, session_verify(SRef, "table:test")),
                        ?assertMatch({ok, <<"apple">>},
                                     session_get(SRef, "table:test", <<"a">>))
                end},
               {"session sync",
                fun() ->
                        ?assertMatch(ok, session_sync(SRef, "table:test")),
                        ?assertMatch({ok, <<"apple">>},
                                     session_get(SRef, "table:test", <<"a">>))
                end},
               {"session salvage",
                fun() ->
                        %% ===============================================================
                        %% KEITH: SKIP SALVAGE FOR NOW, THERE IS SOMETHING WRONG.
                        %% ===============================================================
                        %% ok = session_salvage(SRef, "table:test"),
                        %% {ok, <<"apple">>} = session_get(SRef, "table:test", <<"a">>),
                        ok
                end},
               {"session upgrade",
                fun() ->
                        ?assertMatch(ok, session_upgrade(SRef, "table:test")),
                        ?assertMatch({ok, <<"apple">>},
                                     session_get(SRef, "table:test", <<"a">>))
                end},
               {"session rename",
                fun() ->
                        ?assertMatch(ok,
                                     session_rename(SRef, "table:test", "table:new")),
                        ?assertMatch({ok, <<"apple">>},
                                     session_get(SRef, "table:new", <<"a">>)),
                        ?assertMatch(ok,
                                     session_rename(SRef, "table:new", "table:test")),
                        ?assertMatch({ok, <<"apple">>},
                                     session_get(SRef, "table:test", <<"a">>))
                end},
               {"session truncate",
                fun() ->
                        ?assertMatch(ok, session_truncate(SRef, "table:test")),
                        ?assertMatch(not_found, session_get(SRef, "table:test", <<"a">>))
                end}]}
     end}.

cursor_open_close_test() ->
    {ConnRef, SRef} = init_test_table(),
    {ok, Cursor1} = cursor_open(SRef, "table:test"),
    ?assertMatch({ok, <<"a">>, <<"apple">>}, cursor_next(Cursor1)),
    ?assertMatch(ok, cursor_close(Cursor1)),
    {ok, Cursor2} = cursor_open(SRef, "table:test"),
    ?assertMatch({ok, <<"g">>, <<"gooseberry">>}, cursor_prev(Cursor2)),
    ?assertMatch(ok, cursor_close(Cursor2)),
    stop_test_table({ConnRef, SRef}).

various_cursor_test_() ->
    {setup,
     fun init_test_table/0,
     fun stop_test_table/1,
     fun({_, SRef}) ->
             {inorder,
              [{"move a cursor back and forth, getting key",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch({ok, <<"a">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"b">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"c">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"d">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"c">>}, cursor_prev_key(Cursor)),
                        ?assertMatch({ok, <<"d">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"g">>}, cursor_next_key(Cursor)),
                        ?assertMatch(not_found, cursor_next_key(Cursor)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"move a cursor back and forth, getting value",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch({ok, <<"apple">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"banana">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"cherry">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"date">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"cherry">>}, cursor_prev_value(Cursor)),
                        ?assertMatch({ok, <<"date">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"gooseberry">>}, cursor_next_value(Cursor)),
                        ?assertMatch(not_found, cursor_next_value(Cursor)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"move a cursor back and forth, getting key and value",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch({ok, <<"a">>, <<"apple">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"b">>, <<"banana">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"c">>, <<"cherry">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"d">>, <<"date">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"c">>, <<"cherry">>}, cursor_prev(Cursor)),
                        ?assertMatch({ok, <<"d">>, <<"date">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"g">>, <<"gooseberry">>}, cursor_next(Cursor)),
                        ?assertMatch(not_found, cursor_next(Cursor)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"fold keys",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch([<<"g">>, <<"d">>, <<"c">>, <<"b">>, <<"a">>],
                                     fold_keys(Cursor, fun(Key, Acc) -> [Key | Acc] end, [])),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"search for an item",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch({ok, <<"banana">>}, cursor_search(Cursor, <<"b">>)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"range search for an item",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch({ok, <<"gooseberry">>},
                                     cursor_search_near(Cursor, <<"z">>)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"check cursor reset",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch({ok, <<"apple">>}, cursor_next_value(Cursor)),
                        ?assertMatch(ok, cursor_reset(Cursor)),
                        ?assertMatch({ok, <<"apple">>}, cursor_next_value(Cursor)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"insert/overwrite an item using a cursor",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch(ok,
                                     cursor_insert(Cursor, <<"h">>, <<"huckleberry">>)),
                        ?assertMatch({ok, <<"huckleberry">>},
                                     cursor_search(Cursor, <<"h">>)),
                        ?assertMatch(ok,
                                     cursor_insert(Cursor, <<"g">>, <<"grapefruit">>)),
                        ?assertMatch({ok, <<"grapefruit">>},
                                     cursor_search(Cursor, <<"g">>)),
                        ?assertMatch(ok, cursor_close(Cursor)),
                        ?assertMatch({ok, <<"grapefruit">>},
                                     session_get(SRef, "table:test", <<"g">>)),
                        ?assertMatch({ok, <<"huckleberry">>},
                                     session_get(SRef, "table:test", <<"h">>))
                end},
               {"update an item using a cursor",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch(ok,
                                     cursor_update(Cursor, <<"g">>, <<"goji berries">>)),
                        ?assertMatch(not_found,
                                     cursor_update(Cursor, <<"k">>, <<"kumquat">>)),
                        ?assertMatch(ok, cursor_close(Cursor)),
                        ?assertMatch({ok, <<"goji berries">>},
                                     session_get(SRef, "table:test", <<"g">>))
                end},
               {"remove an item using a cursor",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch(ok,
                                     cursor_remove(Cursor, <<"g">>, <<"goji berries">>)),
                        ?assertMatch(not_found,
                                     cursor_remove(Cursor, <<"l">>, <<"lemon">>)),
                        ?assertMatch(ok, cursor_close(Cursor)),
                        ?assertMatch(not_found,
                                     session_get(SRef, "table:test", <<"g">>))
                end}]}
     end}.

-ifdef(EQC).

qc(P) ->
    ?assert(eqc:quickcheck(?QC_OUT(P))).

keys() ->
    eqc_gen:non_empty(list(eqc_gen:non_empty(binary()))).

values() ->
    eqc_gen:non_empty(list(binary())).

ops(Keys, Values) ->
    {oneof([put, delete]), oneof(Keys), oneof(Values)}.

apply_kv_ops([], _SRef, _Tbl, Acc0) ->
    Acc0;
apply_kv_ops([{put, K, V} | Rest], SRef, Tbl, Acc0) ->
    ok = lsm_tree:session_put(SRef, Tbl, K, V),
    apply_kv_ops(Rest, SRef, Tbl, orddict:store(K, V, Acc0));
apply_kv_ops([{delete, K, _} | Rest], SRef, Tbl, Acc0) ->
    ok = case lsm_tree:session_delete(SRef, Tbl, K) of
             ok ->
                 ok;
             not_found ->
                 ok;
             Else ->
                 Else
         end,
    apply_kv_ops(Rest, SRef, Tbl, orddict:store(K, deleted, Acc0)).

prop_put_delete() ->
    ?LET({Keys, Values}, {keys(), values()},
         ?FORALL(Ops, eqc_gen:non_empty(list(ops(Keys, Values))),
                 begin
                     DataDir = "/tmp/lsm_tree.putdelete.qc",
                     Table = "table:eqc",
                     ?cmd("rm -rf "++DataDir),
                     ok = filelib:ensure_dir(filename:join(DataDir, "x")),
                     Cfg = lsm_tree:config_to_bin([{create,true}]),
                     {ok, Conn} = lsm_tree:conn_open(DataDir, Cfg),
                     {ok, SRef} = lsm_tree:session_open(Conn),
                     try
                         lsm_tree:session_create(SRef, Table),
                         Model = apply_kv_ops(Ops, SRef, Table, []),

                         %% Validate that all deleted values return not_found
                         F = fun({K, deleted}) ->
                                     ?assertEqual(not_found, lsm_tree:session_get(SRef, Table, K));
                                ({K, V}) ->
                                     ?assertEqual({ok, V}, lsm_tree:session_get(SRef, Table, K))
                             end,
                         lists:map(F, Model),
                         true
                     after
                         lsm_tree:session_close(SRef),
                         lsm_tree:conn_close(Conn)
                     end
                 end)).

prop_put_delete_test_() ->
    {timeout, 3*60, fun() -> qc(prop_put_delete()) end}.

-endif.

-endif.
