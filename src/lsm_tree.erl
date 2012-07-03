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

-export([  open/1, open/2
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
         , upgrade/1
         , salvage/1
         , flush/1
         , checkpoint/1
         , compact/1
         , optimize/1
         , truncate/1
         , verify/1
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
         , cursor_delete/1
         % TODO is_empty/1, count/1, size/1, txn_begin/2, txn_begin/3, txn_commit/2,
         % TODO either transact/? or txn_abort/2, snapshot/1, stat/2
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

-spec open(string()) -> {ok, tree()} | {error, term()}.
open(Path) ->
    %% TODO all these values are guesses, they need to be tested and validated
    open(Path, [ {create_if_missing, true}
               , {checksum_values, true}
               , {write_buffer, 104857600} % 10MB
               , {page_size, 4096}
               , {block_size, 104857600} % 10MB
               , {log_size, 104857600} % 10MB
               , {safety, normal}
               , {autowork, on}
%TODO               , {mmap, on}
               , {log, on}
               , {nmerge, 8} ]).

-spec open(string(), config_list()) -> {ok, tree()} | {error, term()}.
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

-spec delete(tree(), key()) -> ok | not_found | {error, term()}.
delete(_Ref, _Key) ->
    ?nif_stub.

-spec truncate(tree()) -> ok | {error, term()}.
truncate(_Ref) ->
    ?nif_stub.

-spec verify(tree()) -> ok | {error, term()}.
verify(_Ref) ->
    ?nif_stub.

-spec salvage(string()) -> ok | {error, term()}.
salvage(_Path) ->
    ?nif_stub.

-spec flush(tree()) -> ok | {error, term()}.
flush(_Ref) ->
    ?nif_stub.

-spec checkpoint(tree()) -> ok | {error, term()}.
checkpoint(_Ref) ->
    ?nif_stub.

-spec optimize(tree()) -> ok | {error, term()}.
optimize(Ref) ->
    compact(Ref).

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

-spec cursor_position(cursor(), key()) -> ok | {ok, value()} | {error, term()}.
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

-spec cursor_delete(cursor()) -> ok | {error, term()}.
cursor_delete(_Ref) ->
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

-spec fold_values(cursor(), fold_values_fun(), any()) -> any().
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
        ?CALL(_From, {fold_result, PID, _, _}) ->
            drain_worker_and_return(MRef, PID, Value);
        {'DOWN', MRef, _, _, _} ->
            Value;
        ?CAST(_, {fold_limit, PID, _}) ->
            erlang:demonitor(MRef, [flush]),
            Value;
        ?CAST(_, {fold_done, PID}) ->
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

reset_db(DataDir) ->
    ?assertCmd("rm -rf "++DataDir),
    ?assertMatch(ok, filelib:ensure_dir(filename:join(DataDir, "x"))).

open_db(DataDir) ->
    %% 104857600 bytes == 10MB
    reset_db(DataDir),
    {ok, Ref} = ?MODULE:open(DataDir, [{create,true},{cache_size,104857600}]),
    Ref.

open_test_config(DataDir) ->
    reset_db(DataDir),
    {ok, Ref} = ?MODULE:open(DataDir, [{create,true},{cache_size,104857600}]),
    %% TODO
    Ref.

create_a_new_database_test() ->
    Ref = open_db(?TEST_DATA_DIR),
    ?assertMatch(ok, ?MODULE:close(Ref)).

insert_delete_test() ->
    Ref = open_db(?TEST_DATA_DIR),
    ?assertMatch(ok, ?MODULE:put(Ref, <<"a">>, <<"apple">>)),
    ?assertMatch({ok, <<"apple">>}, ?MODULE:get(Ref, <<"a">>)),
    ?assertMatch(ok,  ?MODULE:delete(Ref, <<"a">>)),
    ?assertMatch(not_found,  ?MODULE:get(Ref, <<"a">>)),
    ok = ?MODULE:close(Ref),
    ok = ?MODULE:close(Ref).

init_test_table() ->
    Ref = open_db(?TEST_DATA_DIR),
    ?assertMatch(ok, ?MODULE:put(Ref, <<"a">>, <<"apple">>)),
    ?assertMatch(ok, ?MODULE:put(Ref, <<"b">>, <<"banana">>)),
    ?assertMatch(ok, ?MODULE:put(Ref, <<"c">>, <<"cherry">>)),
    ?assertMatch(ok, ?MODULE:put(Ref, <<"d">>, <<"date">>)),
    ?assertMatch(ok, ?MODULE:put(Ref, <<"g">>, <<"gooseberry">>)),
    {Ref, Ref}.

stop_test_table({Ref, Ref}) ->
    ?assertMatch(ok, ?MODULE:close(Ref)),
    ?assertMatch(ok, ?MODULE:close(Ref)).

various_session_test_() ->
    {setup,
     fun init_test_table/0,
     fun stop_test_table/1,
     fun({_, Ref}) ->
             {inorder,
              [{"session verify",
                fun() ->
                        ?assertMatch(ok, ?MODULE:verify(Ref)),
                        ?assertMatch({ok, <<"apple">>}, ?MODULE:get(Ref, <<"a">>))
                end},
               {"session sync",
                fun() ->
                        ?assertMatch(ok, ?MODULE:sync(Ref)),
                        ?assertMatch({ok, <<"apple">>}, ?MODULE:get(Ref, <<"a">>))
                end},
               {"session salvage",
                fun() ->
                        ok = ?MODULE:salvage(Ref),
                        {ok, <<"apple">>} = ?MODULE:get(Ref, <<"a">>)
                end},
               {"session upgrade",
                fun() ->
                        ?assertMatch(ok, ?MODULE:upgrade(Ref)),
                        ?assertMatch({ok, <<"apple">>}, ?MODULE:get(Ref, <<"a">>))
                end},
               {"session truncate",
                fun() ->
                        ?assertMatch(ok, ?MODULE:truncate(Ref)),
                        ?assertMatch(not_found, ?MODULE:get(Ref, <<"a">>))
                end}]}
     end}.

cursor_open_close_test() ->
    {Ref, Ref} = init_test_table(),
    {ok, Cursor1} = ?MODULE:cursor_open(Ref),
    ?assertMatch({ok, <<"a">>, <<"apple">>}, ?MODULE:cursor_next(Cursor1)),
    ?assertMatch(ok, ?MODULE:cursor_close(Cursor1)),
    {ok, Cursor2} = ?MODULE:cursor_open(Ref),
    ?assertMatch({ok, <<"g">>, <<"gooseberry">>}, ?MODULE:cursor_prev(Cursor2)),
    ?assertMatch(ok, ?MODULE:cursor_close(Cursor2)),
    stop_test_table({Ref, Ref}).

various_cursor_test_() ->
    {setup,
     fun init_test_table/0,
     fun stop_test_table/1,
     fun({_, Ref}) ->
             {inorder,
              [{"move a cursor back and forth, getting key",
                fun() ->
                        {ok, Cursor} = ?MODULE:cursor_open(Ref),
                        ?assertMatch({ok, <<"a">>}, ?MODULE:cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"b">>}, ?MODULE:cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"c">>}, ?MODULE:cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"d">>}, ?MODULE:cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"c">>}, ?MODULE:cursor_prev_key(Cursor)),
                        ?assertMatch({ok, <<"d">>}, ?MODULE:cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"g">>}, ?MODULE:cursor_next_key(Cursor)),
                        ?assertMatch(not_found, ?MODULE:cursor_next_key(Cursor)),
                        ?assertMatch(ok, ?MODULE:cursor_close(Cursor))
                end},
               {"move a cursor back and forth, getting value",
                fun() ->
                        {ok, Cursor} = ?MODULE:cursor_open(Ref),
                        ?assertMatch({ok, <<"apple">>}, ?MODULE:cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"banana">>}, ?MODULE:cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"cherry">>}, ?MODULE:cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"date">>}, ?MODULE:cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"cherry">>}, ?MODULE:cursor_prev_value(Cursor)),
                        ?assertMatch({ok, <<"date">>}, ?MODULE:cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"gooseberry">>}, ?MODULE:cursor_next_value(Cursor)),
                        ?assertMatch(not_found, ?MODULE:cursor_next_value(Cursor)),
                        ?assertMatch(ok, ?MODULE:cursor_close(Cursor))
                end},
               {"move a cursor back and forth, getting key and value",
                fun() ->
                        {ok, Cursor} = ?MODULE:cursor_open(Ref),
                        ?assertMatch({ok, <<"a">>, <<"apple">>}, ?MODULE:cursor_next(Cursor)),
                        ?assertMatch({ok, <<"b">>, <<"banana">>}, ?MODULE:cursor_next(Cursor)),
                        ?assertMatch({ok, <<"c">>, <<"cherry">>}, ?MODULE:cursor_next(Cursor)),
                        ?assertMatch({ok, <<"d">>, <<"date">>}, ?MODULE:cursor_next(Cursor)),
                        ?assertMatch({ok, <<"c">>, <<"cherry">>}, ?MODULE:cursor_prev(Cursor)),
                        ?assertMatch({ok, <<"d">>, <<"date">>}, ?MODULE:cursor_next(Cursor)),
                        ?assertMatch({ok, <<"g">>, <<"gooseberry">>}, ?MODULE:cursor_next(Cursor)),
                        ?assertMatch(not_found, ?MODULE:cursor_next(Cursor)),
                        ?assertMatch(ok, ?MODULE:cursor_close(Cursor))
                end},
               {"fold keys",
                fun() ->
                        {ok, Cursor} = ?MODULE:cursor_open(Ref),
                        ?assertMatch([<<"g">>, <<"d">>, <<"c">>, <<"b">>, <<"a">>],
                                     fold_keys(Cursor, fun(Key, Acc) -> [Key | Acc] end, [])),
                        ?assertMatch(ok, ?MODULE:cursor_close(Cursor))
                end},
               {"search for an item",
                fun() ->
                        {ok, Cursor} = ?MODULE:cursor_open(Ref),
                        ?assertMatch({ok, <<"banana">>}, ?MODULE:cursor_search(Cursor, <<"b">>)),
                        ?assertMatch(ok, ?MODULE:cursor_close(Cursor))
                end},
               {"range search for an item",
                fun() ->
                        {ok, Cursor} = ?MODULE:cursor_open(Ref),
                        ?assertMatch({ok, <<"gooseberry">>},
                                     ?MODULE:cursor_search_near(Cursor, <<"z">>)),
                        ?assertMatch(ok, ?MODULE:cursor_close(Cursor))
                end},
               {"check cursor reset",
                fun() ->
                        {ok, Cursor} = ?MODULE:cursor_open(Ref),
                        ?assertMatch({ok, <<"apple">>}, ?MODULE:cursor_next_value(Cursor)),
                        ?assertMatch(ok, ?MODULE:cursor_reset(Cursor)),
                        ?assertMatch({ok, <<"apple">>}, ?MODULE:cursor_next_value(Cursor)),
                        ?assertMatch(ok, ?MODULE:cursor_close(Cursor))
                end},
               {"insert/overwrite an item using a cursor",
                fun() ->
                        {ok, Cursor} = ?MODULE:cursor_open(Ref),
                        ?assertMatch(ok,
                                     ?MODULE:cursor_insert(Cursor, <<"h">>, <<"huckleberry">>)),
                        ?assertMatch({ok, <<"huckleberry">>},
                                     ?MODULE:cursor_search(Cursor, <<"h">>)),
                        ?assertMatch(ok,
                                     ?MODULE:cursor_insert(Cursor, <<"g">>, <<"grapefruit">>)),
                        ?assertMatch({ok, <<"grapefruit">>},
                                     ?MODULE:cursor_search(Cursor, <<"g">>)),
                        ?assertMatch(ok, ?MODULE:cursor_close(Cursor)),
                        ?assertMatch({ok, <<"grapefruit">>},
                                     ?MODULE:get(Ref, <<"g">>)),
                        ?assertMatch({ok, <<"huckleberry">>},
                                     ?MODULE:get(Ref, <<"h">>))
                end},
               {"update an item using a cursor",
                fun() ->
                        {ok, Cursor} = ?MODULE:cursor_open(Ref),
                        ?assertMatch(ok,
                                     ?MODULE:cursor_update(Cursor, <<"g">>, <<"goji berries">>)),
                        ?assertMatch(not_found,
                                     ?MODULE:cursor_update(Cursor, <<"k">>, <<"kumquat">>)),
                        ?assertMatch(ok, ?MODULE:cursor_close(Cursor)),
                        ?assertMatch({ok, <<"goji berries">>},
                                     ?MODULE:get(Ref, <<"g">>))
                end},
               {"remove an item using a cursor",
                fun() ->
                        {ok, Cursor} = ?MODULE:cursor_open(Ref),
                        ?assertMatch(ok,
                                     ?MODULE:cursor_remove(Cursor, <<"g">>, <<"goji berries">>)),
                        ?assertMatch(not_found,
                                     ?MODULE:cursor_remove(Cursor, <<"l">>, <<"lemon">>)),
                        ?assertMatch(ok, ?MODULE:cursor_close(Cursor)),
                        ?assertMatch(not_found,
                                     ?MODULE:get(Ref, <<"g">>))
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

apply_kv_ops([], _Ref,  Acc0) ->
    Acc0;
apply_kv_ops([{put, K, V} | Rest], Ref, Acc0) ->
    ok = ?MODULE:put(Ref, K, V),
    apply_kv_ops(Rest, Ref, orddict:store(K, V, Acc0));
apply_kv_ops([{delete, K, _} | Rest], Ref, Acc0) ->
    ok = case ?MODULE:delete(Ref, K) of
             ok ->
                 ok;
             not_found ->
                 ok;
             Else ->
                 Else
         end,
    apply_kv_ops(Rest, Ref, orddict:store(K, deleted, Acc0)).

prop_put_delete() ->
    ?LET({Keys, Values}, {keys(), values()},
         ?FORALL(Ops, eqc_gen:non_empty(list(ops(Keys, Values))),
                 begin
                     DataDir = "/tmp/lsm_tree.putdelete.qc",
                     ?cmd("rm -rf "++DataDir),
                     ok = filelib:ensure_dir(filename:join(DataDir, "x")),
                     {ok, Ref} = ?MODULE:open(DataDir, [{create, true}]),
                     try
                         Model = apply_kv_ops(Ops, Ref, []),

                         %% Validate that all deleted values return not_found
                         F = fun({K, deleted}) ->
                                     ?assertEqual(not_found, ?MODULE:get(Ref, K));
                                ({K, V}) ->
                                     ?assertEqual({ok, V}, ?MODULE:get(Ref, K))
                             end,
                         lists:map(F, Model),
                         true
                     after
                         ?MODULE:close(Re),
                     end
                 end)).

prop_put_delete_test_() ->
    {timeout, 3*60, fun() -> qc(prop_put_delete()) end}.

-endif.

-endif.
