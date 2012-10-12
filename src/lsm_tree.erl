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
         , cursor_seek/2
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
         , transact/2
         % TODO snapshot/1, stat/2
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

-opaque tree() :: reference().
-opaque cursor() :: reference().
-type config_list() :: [{atom(), any()}].
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

-spec cursor_seek(cursor(), key()) -> ok | {ok, value()} | {error, term()}.
cursor_seek(Cursor, Key) ->
    cursor_position(Cursor, Key).

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

-type transact_spec() :: {put, key(), value()} | {delete, value()}.

-spec transact(tree(), [transact_spec()]) -> ok | {error, term()}.
transact(_Tree, _TransactionSpec) ->
    ?nif_stub.

-type fold_keys_fun() :: fun((key(), any()) -> any()).

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

-type fold_values_fun() :: fun((key(), any()) -> any()).

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

-type fold_fun() :: fun(({key(), value()}, any()) -> any()).

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

-define(TEST_DATAFILE, "lsm_tree.basic").

reset_db(DataFile) ->
    file:delete(DataFile),
    file:delete(filename:join(DataFile, "-log")).

open_db(DataFile) ->
    {ok, Tree} = lsm_tree:open("lsm_tree.basic"),
    Tree.

open_test_config(DataFile) ->
    {ok, Tree} = lsm_tree:open(DataFile),
    %% TODO
    Tree.

create_a_new_database_test() ->
    reset_db(?TEST_DATAFILE),
    Tree = open_db(?TEST_DATAFILE),
    ?assertMatch(ok, lsm_tree:close(Tree)).

insert_delete_test() ->
    reset_db(?TEST_DATAFILE),
    Tree = open_db(?TEST_DATAFILE),
    ?assertMatch(ok, lsm_tree:put(Tree, <<"a">>, <<"apple">>)),
    ?assertMatch({ok, <<"apple">>}, lsm_tree:get(Tree, <<"a">>)),
    ?assertMatch(ok, lsm_tree:delete(Tree, <<"a">>)),
    ?assertMatch(not_found, lsm_tree:get(Tree, <<"a">>)),
    ?assertMatch(ok, lsm_tree:close(Tree)),
    ok.

init_test_table() ->
    reset_db(?TEST_DATAFILE),
    Tree = open_db(?TEST_DATAFILE),
    ?assertMatch(ok, lsm_tree:put(Tree, <<"a">>, <<"apple">>)),
    ?assertMatch(ok, lsm_tree:put(Tree, <<"b">>, <<"banana">>)),
    ?assertMatch(ok, lsm_tree:put(Tree, <<"c">>, <<"cherry">>)),
    ?assertMatch(ok, lsm_tree:put(Tree, <<"d">>, <<"date">>)),
    ?assertMatch(ok, lsm_tree:put(Tree, <<"g">>, <<"gooseberry">>)),
    {Tree, Tree}.

stop_test_table({Tree, Tree}) ->
    ?assertMatch(ok, lsm_tree:close(Tree)).

%various_session_test_() ->
%    {setup,
%     fun init_test_table/0,
%     fun stop_test_table/1,
%     fun({_, Tree}) ->
%             {inorder,
%             [
% TODO:       {"session verify",
%                fun() ->
%                        ?assertMatch(ok, lsm_tree:verify(Tree)),
%                        ?assertMatch({ok, <<"apple">>}, lsm_tree:get(Tree, <<"a">>))
%                end}
% TODO:       ,{"session flush",
%                fun() ->
%                        ?assertMatch(ok, lsm_tree:flush(Tree)),
%                        ?assertMatch({ok, <<"apple">>}, lsm_tree:get(Tree, <<"a">>))
%                end}
% TODO:       ,{"session salvage",
%                fun() ->
%                        ok = lsm_tree:salvage(Tree),
%                        {ok, <<"apple">>} = lsm_tree:get(Tree, <<"a">>)
%                end}
% TODO:       ,{"session upgrade",
%                fun() ->
%                        ?assertMatch(ok, lsm_tree:upgrade(Tree)),
%                        ?assertMatch({ok, <<"apple">>}, lsm_tree:get(Tree, <<"a">>))
%                end}
% TODO:       ,{"session truncate",
%                fun() ->
%                        ?assertMatch(ok, lsm_tree:truncate(Tree)),
%                        ?assertMatch(not_found, lsm_tree:get(Tree, <<"a">>))
%                end}
%              ]}
%     end}.

cursor_open_pos_get_then_close_test() ->
    {Tree, Tree} = init_test_table(),
    {ok, Cursor1} = lsm_tree:cursor_open(Tree),
    ?assertMatch(ok, lsm_tree:cursor_first(Cursor1)),
    ?assertMatch({ok, <<"a">>, <<"apple">>}, lsm_tree:cursor_next(Cursor1)),
    ?assertMatch(ok, lsm_tree:cursor_close(Cursor1)),
    {ok, Cursor2} = lsm_tree:cursor_open(Tree),
    ?assertMatch(ok, lsm_tree:cursor_last(Cursor2)),
    ?assertMatch({ok, <<"g">>, <<"gooseberry">>}, lsm_tree:cursor_prev(Cursor2)),
    ?assertMatch(ok, lsm_tree:cursor_close(Cursor2)),
    stop_test_table({Tree, Tree}).

various_cursor_test_() ->
    {setup,
     fun init_test_table/0,
     fun stop_test_table/1,
     fun({_, Tree}) ->
             {inorder,
              [{"move a cursor forward, getting key",
                fun() ->
                        {ok, Cursor} = lsm_tree:cursor_open(Tree),
                        ?assertMatch(ok, lsm_tree:cursor_first(Cursor)),
                        ?assertMatch({ok, <<"a">>}, lsm_tree:cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"b">>}, lsm_tree:cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"c">>}, lsm_tree:cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"d">>}, lsm_tree:cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"g">>}, lsm_tree:cursor_next_key(Cursor)),
                        ?assertMatch(not_found, lsm_tree:cursor_next_key(Cursor)),
                        ?assertMatch(ok, lsm_tree:cursor_close(Cursor))
                end},
               {"move a cursor forward, getting value",
                fun() ->
                        {ok, Cursor} = lsm_tree:cursor_open(Tree),
                        ?assertMatch(ok, lsm_tree:cursor_first(Cursor)),
                        ?assertMatch({ok, <<"apple">>}, lsm_tree:cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"banana">>}, lsm_tree:cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"cherry">>}, lsm_tree:cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"date">>}, lsm_tree:cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"gooseberry">>}, lsm_tree:cursor_next_value(Cursor)),
                        ?assertMatch(not_found, lsm_tree:cursor_next_value(Cursor)),
                        ?assertMatch(ok, lsm_tree:cursor_close(Cursor))
                end},
               {"move a cursor forward, getting key and value",
                fun() ->
                        {ok, Cursor} = lsm_tree:cursor_open(Tree),
                        ?assertMatch(ok, lsm_tree:cursor_first(Cursor)),
                        ?assertMatch({ok, <<"a">>, <<"apple">>}, lsm_tree:cursor_next(Cursor)),
                        ?assertMatch({ok, <<"b">>, <<"banana">>}, lsm_tree:cursor_next(Cursor)),
                        ?assertMatch({ok, <<"c">>, <<"cherry">>}, lsm_tree:cursor_next(Cursor)),
                        ?assertMatch({ok, <<"d">>, <<"date">>}, lsm_tree:cursor_next(Cursor)),
                        ?assertMatch({ok, <<"g">>, <<"gooseberry">>}, lsm_tree:cursor_next(Cursor)),
                        ?assertMatch(not_found, lsm_tree:cursor_next(Cursor)),
                        ?assertMatch(ok, lsm_tree:cursor_close(Cursor))
                end},
               {"fold keys",
                fun() ->
                        {ok, Cursor} = lsm_tree:cursor_open(Tree),
                        ?assertMatch(ok, lsm_tree:cursor_first(Cursor)),
                        ?assertMatch([<<"g">>, <<"d">>, <<"c">>, <<"b">>, <<"a">>],
                                     fold_keys(Cursor, fun(Key, Acc) -> [Key | Acc] end, [])),
                        ?assertMatch(ok, lsm_tree:cursor_close(Cursor))
                end},
               {"search for an item",
                fun() ->
                        {ok, Cursor} = lsm_tree:cursor_open(Tree),
                        ?assertMatch(ok, lsm_tree:cursor_first(Cursor)),
                        ?assertMatch(ok, lsm_tree:cursor_position(Cursor, <<"b">>)),
                        ?assertMatch(ok, lsm_tree:cursor_close(Cursor))
                end},
               {"check cursor re-position",
                fun() ->
                        {ok, Cursor} = lsm_tree:cursor_open(Tree),
                        ?assertMatch(ok, lsm_tree:cursor_first(Cursor)),
                        ?assertMatch({ok, <<"apple">>}, lsm_tree:cursor_next_value(Cursor)),
                        ?assertMatch(ok, lsm_tree:cursor_first(Cursor)),
                        ?assertMatch({ok, <<"apple">>}, lsm_tree:cursor_next_value(Cursor)),
                        ?assertMatch(ok, lsm_tree:cursor_close(Cursor))
                end},
               {"insert/overwrite an item using a cursor",
                fun() ->
                        {ok, Cursor} = lsm_tree:cursor_open(Tree),
                        ?assertMatch(ok, lsm_tree:cursor_first(Cursor)),
                        ?assertMatch(ok, lsm_tree:put(Tree, <<"h">>, <<"huckleberry">>)),
                        ?assertMatch(ok, lsm_tree:cursor_position(Cursor, <<"h">>)),
% TODO:                 ?assertMatch({ok, <<"huckleberry">>}, lsm_tree:cursor_next_value(Cursor)),
                        ?assertMatch(ok, lsm_tree:put(Tree, <<"g">>, <<"grapefruit">>)),
                        ?assertMatch(ok, lsm_tree:cursor_position(Cursor, <<"g">>)),
% TODO:                 ?assertMatch({ok, <<"grapefruit">>}, lsm_tree:cursor_next_value(Cursor)),
                        ?assertMatch(ok, lsm_tree:cursor_close(Cursor)),
                        ?assertMatch({ok, <<"grapefruit">>}, lsm_tree:get(Tree, <<"g">>)),
                        ?assertMatch({ok, <<"huckleberry">>}, lsm_tree:get(Tree, <<"h">>))
                end}
              ]}
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

apply_kv_ops([], _Tree,  Acc0) ->
    Acc0;
apply_kv_ops([{put, K, V} | Rest], Tree, Acc0) ->
    ok = lsm_tree:put(Tree, K, V),
    apply_kv_ops(Rest, Tree, orddict:store(K, V, Acc0));
apply_kv_ops([{delete, K, _} | Rest], Tree, Acc0) ->
    ok = case lsm_tree:delete(Tree, K) of
             ok ->
                 ok;
             not_found ->
                 ok;
             Else ->
                 Else
         end,
    apply_kv_ops(Rest, Tree, orddict:store(K, deleted, Acc0)).

prop_put_delete() ->
    ?LET({Keys, Values}, {keys(), values()},
         ?FORALL(Ops, eqc_gen:non_empty(list(ops(Keys, Values))),
                 begin
                     DataDir = "/tmp/lsm_tree.putdelete.qc/file.dat",
                     ?cmd("rm -rf "++DataDir),
                     ok = filelib:ensure_dir(DataDir),
                     {ok, Tree} = lsm_tree:open(DataDir, [{create, true}]),
                     try
                         Model = apply_kv_ops(Ops, Tree, []),

                         %% Validate that all deleted values return not_found
                         F = fun({K, deleted}) ->
                                     ?assertEqual(not_found, lsm_tree:get(Tree, K));
                                ({K, V}) ->
                                     ?assertEqual({ok, V}, lsm_tree:get(Tree, K))
                             end,
                         lists:map(F, Model),
                         true
                     after
                         lsm_tree:close(Tree)
                     end
                 end)).

prop_put_delete_test_() ->
    {timeout, 3*60, fun() -> qc(prop_put_delete()) end}.

-endif.

-endif.
