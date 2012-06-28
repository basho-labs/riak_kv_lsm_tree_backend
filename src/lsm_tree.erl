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
-export([open/2,
         close/1,
         get/2,
         put/3,
         delete/3,
         fold/3,
         fold_keys/3,
         truncate/1,
         config_option/0]).

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
-opaque connection() :: reference().
-opaque cursor() :: reference().
-type key() :: binary().
-type value() :: binary().

-export_type([connection/0, cursor/0]).

-on_load(init/0).

-define(nif_stub, nif_stub_error(?LINE)).
nif_stub_error(Line) ->
    erlang:nif_error({nif_not_loaded,module,?MODULE,line,Line}).

-define(EMPTY_CONFIG, <<"\0">>).

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

-spec open(string(), config()) -> {ok, connection()} | {error, term()}.
open(_HomeDir, _Config) ->
    ?nif_stub.

-spec close(connection()) -> ok | {error, term()}.
close(_ConnRef) ->
    ?nif_stub.

-spec delete(session(), string(), key()) -> ok | {error, term()}.
delete(_Ref, _Table, _Key) ->
    ?nif_stub.

-spec get(session(), string(), key()) -> {ok, value()} | not_found | {error, term()}.
get(_Ref, _Table, _Key) ->
    ?nif_stub.

-spec put(session(), string(), key(), value()) -> ok | {error, term()}.
put(_Ref, _Table, _Key, _Value) ->
    ?nif_stub.

-spec truncate(session(), string()) -> ok | {error, term()}.
-spec truncate(session(), string(), config()) -> ok | {error, term()}.
truncate(Ref, Name) ->
    session_truncate(Ref, Name, ?EMPTY_CONFIG).
truncate(_Ref, _Name, _Config) ->
    ?nif_stub.

-spec verify(session(), string()) -> ok | {error, term()}.
-spec verify(session(), string(), config()) -> ok | {error, term()}.
verify(Ref, Name) ->
    verify(Ref, Name, ?EMPTY_CONFIG).
verify(_Ref, _Name, _Config) ->
    ?nif_stub.

-spec cursor_open(session(), string()) -> {ok, cursor()} | {error, term()}.
cursor_open(_Ref, _Table) ->
    ?nif_stub.

-spec cursor_close(cursor()) -> ok | {error, term()}.
cursor_close(_Cursor) ->
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

-spec cursor_search(cursor(), key()) -> {ok, value()} | {error, term()}.
cursor_search(_Cursor, _Key) ->
    ?nif_stub.

-spec cursor_search_near(cursor(), key()) -> {ok, value()} | {error, term()}.
cursor_search_near(_Cursor, _Key) ->
    ?nif_stub.

-spec cursor_reset(cursor()) -> ok | {error, term()}.
cursor_reset(_Cursor) ->
    ?nif_stub.

-spec cursor_insert(cursor(), key(), value()) -> ok | {error, term()}.
cursor_insert(_Cursor, _Key, _Value) ->
    ?nif_stub.

-spec cursor_update(cursor(), key(), value()) -> ok | {error, term()}.
cursor_update(_Cursor, _Key, _Value) ->
    ?nif_stub.

-spec cursor_remove(cursor(), key(), value()) -> ok | {error, term()}.
cursor_remove(_Cursor, _Key, _Value) ->
    ?nif_stub.

-type fold_keys_fun() :: fun((Key::binary(), any()) -> any()).

-spec fold_keys(cursor(), fold_keys_fun(), any()) -> any().
fold_keys(Cursor, Fun, Acc0) ->
    fold_keys(Cursor, Fun, Acc0, cursor_next_key(Cursor)).
fold_keys(_Cursor, _Fun, Acc, not_found) ->
    Acc;
fold_keys(Cursor, Fun, Acc, {ok, Key}) ->
    fold_keys(Cursor, Fun, Fun(Key, Acc), cursor_next_key(Cursor)).

-type fold_fun() :: fun(({Key::binary(), Value::binary()}, any()) -> any()).

-spec fold(cursor(), fold_fun(), any()) -> any().
fold(Cursor, Fun, Acc0) ->
    fold(Cursor, Fun, Acc0, cursor_next(Cursor)).
fold(_Cursor, _Fun, Acc, not_found) ->
    Acc;
fold(Cursor, Fun, Acc, {ok, Key, Value}) ->
    fold(Cursor, Fun, Fun({Key, Value}, Acc), cursor_next(Cursor)).

%%
%% Configuration type information.
%%
config_types() ->
    [{cache_size, string},
     {create, bool},
     {error_prefix, string},
     {eviction_target, integer},
     {eviction_trigger, integer},
     {extensions, string},
     {force, bool},
     {hazard_max, integer},
     {home_environment, bool},
     {home_environment_priv, bool},
     {logging, bool},
     {multiprocess, bool},
     {session_max, integer},
     {transactional, bool},
     {verbose, string}].


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
