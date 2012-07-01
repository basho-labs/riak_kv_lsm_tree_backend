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


-type open_options() :: [

%%  Create specified database filename (if path where specified  exists).
                           {create_if_missing | create, boolean()}

%% Should attempting to open an exiting database be an error?
                         | {error_if_exists, boolean()}

%% Should each value be wrapped in a crc32 checksum as it is stored and
%% then that value checked to ensure data integrity when retrieved?
                         | {checksum_values, boolean()}

%%   write_buffer == LSM_CONFIG_WRITE_BUFFER
%%     A read/write integer parameter. This value determines the maximum amount
%%     of space (in bytes) used to accumulate writes in main-memory before
%%     they are flushed to a level 0 segment.
%%     limits: if (n < 8KB || n > (512 * MB)) n = 8KB;
                         | {write_buffer, pos_integer()}

%%   page_size == LSM_CONFIG_PAGE_SIZE
%%     A read/write integer parameter. This parameter may only be set before
%%     lsm_open() has been called.
%%     limits: if (n < 512 || n > 8KB) n = 8KB;
                         | {page_size, pos_integer()}

%%   block_size == LSM_CONFIG_BLOCK_SIZE
%%     A read/write integer parameter. This parameter may only be set before
%%     lsm_open() has been called.
%%     limits: if (n < 8KB || n > (512 * MB)) n = 8KB;
                         | {block_size, pos_integer()}

%%   log_size == LSM_CONFIG_LOG_SIZE
%%     A read/write integer parameter.
%%     if (n < 8KB || n > (512 * MB)) n = 8KB;
                         | {log_size, pos_integer()}

%%   config_safety == LSM_CONFIG_SAFETY
%%     A read/write integer parameter. Valid values are 0, 1 (the default)
%%     and 2. This parameter determines how robust the database is in the
%%     face of a system crash (e.g. a power failure or operating system
%%     crash). As follows:
%%
%%       0 (off):    No robustness. A system crash may corrupt the database.
%%
%%       1 (normal): Some robustness. A system crash may not corrupt the
%%                   database file, but recently committed transactions may
%%                   be lost following recovery.
%%
%%       2 (full):   Full robustness. A system crash may not corrupt the
%%                   database file. Following recovery the database file
%%                   contains all successfully committed transactions.
                         | {safety, off | normal | full} % 0 | 1 | 2

%%   autowork == LSM_CONFIG_AUTOWORK
%%     A read/write integer parameter. NOTE: see "TODO" comment in c_src/lsm.h
                         | {autowork, off | on} % 0 | 1

%%   mmap == LSM_CONFIG_MMAP
%%     A read/write integer parameter. True to use mmap() to access the
%%     database file. False otherwise.
                         | {mmap, off | on} % 0 | 1

%%   use_log == LSM_CONFIG_USE_LOG
%%     A read/write boolean parameter. True (the default) to use the log
%%     file normally. False otherwise.
                         | {use_log,  off | on} % 0 | 1

%%   nmerge == LSM_CONFIG_NMERGE
%%     A read/write integer parameter. The minimum number of segments to
%%     merge together at a time. Default value 4.
%%     limits: if (n < 4 || n > 100) n = 4;
                         | {nmerge, pos_integer()} % default: 4, max is 1024
                        ].
%%
%% The key_range structure is a bit assymetric, here is why:
%%
%% from_key=<<>> is "less than" any other key, hence we don't need to
%% handle from_key=undefined to support an open-ended start of the
%% interval. For to_key, we cannot (statically) construct a key
%% which is > any possible key, hence we need to allow to_key=undefined
%% as a token of an interval that has no upper limit.
%%
-record(key_range, {   from_key = <<>>       :: binary(),
                       from_inclusive = true :: boolean(),
                       to_key                :: binary() | undefined,
                       to_inclusive = false  :: boolean(),
                       limit :: pos_integer() | undefined }).
-type key_range() :: #key_range{}.
