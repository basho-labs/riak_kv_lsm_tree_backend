// -*- coding: utf-8; Mode: c; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*-
// ex: set softtabstop=4 tabstop=4 shiftwidth=4 expandtab fileencoding=utf-8:

// ----------------------------------------------------------------------------
//
// lsm_tree: A Riak/KV backend using SQLite4's Log-Structured Merge Tree
//
// Copyright 2012 (c) Basho Technologies, Inc.  All Rights Reserved.
// http://basho.com/ info@basho.com
//
// This file is provided to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.
//
// ----------------------------------------------------------------------------

#include "erl_nif.h"
#include "erl_driver.h"

#include <stdio.h>
#include <string.h>
#include <sys/param.h>

#include "lsm.h"
#define LSM_NOTFOUND -1 // TODO: validate this

#define KB 1024
#define MB (1024 * KB)

static ErlNifResourceType* lsm_tree_RESOURCE;
static ErlNifResourceType* lsm_cursor_RESOURCE;
static lsm_env erl_nif_env;

typedef struct {
  lsm_db *pDb;                       /* LSM database handle */
} LsmTreeHandle;

typedef struct {
  lsm_cursor *pCsr;                  /* LSM cursor handle */
} LsmCursorHandle;

// Atoms (initialized in on_load)
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_NOTFOUND;
static ERL_NIF_TERM ATOM_ENOMEM;
static ERL_NIF_TERM ATOM_BADARG;
static ERL_NIF_TERM ATOM_WRITE_BUFFER;
static ERL_NIF_TERM ATOM_PAGE_SIZE;
static ERL_NIF_TERM ATOM_BLOCK_SIZE;
static ERL_NIF_TERM ATOM_LOG_SIZE;
static ERL_NIF_TERM ATOM_SAFETY;
static ERL_NIF_TERM ATOM_AUTOWORK;
static ERL_NIF_TERM ATOM_MMAP;
static ERL_NIF_TERM ATOM_USE_LOG;
static ERL_NIF_TERM ATOM_NMERGE;
static ERL_NIF_TERM ATOM_ON;
static ERL_NIF_TERM ATOM_OFF;
static ERL_NIF_TERM ATOM_NORMAL;
static ERL_NIF_TERM ATOM_FULL;
static ERL_NIF_TERM ATOM_TRUE;
static ERL_NIF_TERM ATOM_FALSE;
static ERL_NIF_TERM ATOM_CREATE; // Shorthand for CREATE_IF_MISSING below
static ERL_NIF_TERM ATOM_CREATE_IF_MISSING; //TODO
static ERL_NIF_TERM ATOM_ERROR_IF_EXISTS;   //TODO
static ERL_NIF_TERM ATOM_CHECKSUM_VALUES;   //TODO

// Prototypes
static ERL_NIF_TERM lsm_tree_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_cursor_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_cursor_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_cursor_position(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_cursor_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_cursor_prev(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_cursor_first(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_cursor_last(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_cursor_next_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_cursor_next_value(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_cursor_prev_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_cursor_prev_value(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
//static ERL_NIF_TERM lsm_txn_begin(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
//static ERL_NIF_TERM lsm_txn_commit(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
//static ERL_NIF_TERM lsm_txn_abort(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_salvage(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_flush(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_checkpoint(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_compact(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_upgrade(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_verify(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
//TODO: static ERL_NIF_TERM lsm_tree_count(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

typedef ERL_NIF_TERM (*CursorRetFun)(ErlNifEnv* env, lsm_cursor* cursor, int rc);

static ErlNifFunc nif_funcs[] =
{
    {"open",              2, lsm_tree_open},
    {"close",             1, lsm_tree_close},
    {"get",               2, lsm_tree_get},
    {"put",               3, lsm_tree_put},
    {"delete",            2, lsm_tree_delete},
    {"flush",             1, lsm_tree_flush},
    {"salvage",           1, lsm_tree_salvage},
    {"checkpoint",        1, lsm_tree_checkpoint},
    {"truncate",          1, lsm_tree_truncate},
    {"compact",           1, lsm_tree_compact},
    {"destroy",           1, lsm_tree_destroy},
    {"upgrade",           1, lsm_tree_upgrade},
    {"verify",            1, lsm_tree_verify},
    {"cursor_open",       1, lsm_cursor_open},
    {"cursor_close",      1, lsm_cursor_close},
    {"cursor_position",   2, lsm_cursor_position},
    {"cursor_next_key",   1, lsm_cursor_next_key},
    {"cursor_next_value", 1, lsm_cursor_next_value},
    {"cursor_prev_key",   1, lsm_cursor_prev_key},
    {"cursor_prev_value", 1, lsm_cursor_prev_value},
    {"cursor_next",       1, lsm_cursor_next},
    {"cursor_prev",       1, lsm_cursor_prev},
    {"cursor_first",      1, lsm_cursor_first},
    {"cursor_last",       1, lsm_cursor_last},
//    {"txn_begin",         2, lsm_txn_begin},
//    {"txn_commit",        2, lsm_txn_commit},
//    {"txn_abort",         2, lsm_txn_abort},
};


static ERL_NIF_TERM make_atom(ErlNifEnv* env, const char* name)
{
  ERL_NIF_TERM ret;
  if(enif_make_existing_atom(env, name, &ret, ERL_NIF_LATIN1))
    return ret;
  return enif_make_atom(env, name);
}

//static ERL_NIF_TERM make_ok(ErlNifEnv* env, ERL_NIF_TERM msg)
//{
//  return enif_make_tuple2(env, ATOM_OK, msg);
//}

#if defined(TEST) || defined(DEBUG)
#define make_error_msg(__env, __msg) __make_error_msg(__env, __msg, __FILE__, __LINE__)
static ERL_NIF_TERM __make_error_msg(ErlNifEnv* env, const char* msg, char *file, int line)
#else
static ERL_NIF_TERM __make_error_msg(ErlNifEnv* env, const char* msg)
#endif
{
#if defined(TEST) || defined(DEBUG)
  static char buf[MAXPATHLEN+1024];
  snprintf(buf, 1024, "%s at %s:%d", msg, file, line);
  return enif_make_tuple2(env, ATOM_ERROR, make_atom(env, msg));
#else
  return enif_make_tuple2(env, ATOM_ERROR, make_atom(env, msg));
#endif
}

#if defined(TEST) || defined(DEBUG)
#define make_error(__env, __rc) __make_error(__env, __rc, __FILE__, __LINE__)
#define __error_msg(__env, __msg, __file, __line) __make_error_msg(__env, __msg, __file, __line)
static ERL_NIF_TERM __make_error(ErlNifEnv* env, int rc, char *file, int line)
#else
#define make_error(__env, __rc) __make_error(__env, __rc)
#define __error_msg(__env, __msg, __file, __line) __make_error_msg(__env, __msg)
static ERL_NIF_TERM __make_error(ErlNifEnv* env, int rc)
#endif
{
  switch(rc)
    {
    case LSM_OK:             return ATOM_OK;
    case LSM_NOTFOUND:       return ATOM_NOTFOUND;
    case LSM_BUSY:           return __error_msg(env, "lsm_busy", file, line);
    case LSM_NOMEM:          return ATOM_ENOMEM;
    case LSM_IOERR:          return __error_msg(env, "lsm_ioerr", file, line);
    case LSM_CORRUPT:        return __error_msg(env, "lsm_corrupt", file, line);
    case LSM_FULL:           return __error_msg(env, "lsm_full", file, line);
    case LSM_CANTOPEN:       return __error_msg(env, "lsm_cant_open", file, line);
    case LSM_MISUSE:         return __error_msg(env, "lsm_misuse", file, line);
    case LSM_ERROR: default:;/* FALLTHRU */
    }
#if defined(TEST) || defined(DEBUG)
  return __error_msg(env, "lsm_error", file, line);
#else
  return __error_msg(env, "lsm_error");
#endif
}

static int __compare_keys(const void *key1, int n1, const void *key2, int n2)
{
    int c = memcmp(key1, key2, ((n1 < n2) ? n1 : n2));
    if (c == 0)
      c = n1 - n2;
    return c;
}

ERL_NIF_TERM __config_lsm_env(ErlNifEnv* env, ERL_NIF_TERM list, int op, lsm_db *db)
{
    int rc = LSM_OK;
    int arity;
    int n;
    ERL_NIF_TERM head, tail;
    const ERL_NIF_TERM* option;
    static char msg[1024], o[1024];

    while (enif_get_list_cell(env, list, &head, &tail)) {
        if (!enif_get_tuple(env, head, &arity, &option)) {
            enif_get_string(env, head, o, sizeof o, ERL_NIF_LATIN1);
            snprintf(msg, 1024, "lsm_tree:open config \"%s\" is not a valid tuple", o);
            return make_error_msg(env, msg);
        }
        if (arity != 2) return make_error_msg(env, "lsm_tree:open_config -- wrong tuple size");
        if (option[0] == ATOM_WRITE_BUFFER && op == LSM_CONFIG_WRITE_BUFFER) {
            if (!enif_get_int(env, option[1], &n) || n < (8 * KB) || n > (512 * MB)) {
                enif_get_string(env, option[1], o, sizeof o, ERL_NIF_LATIN1);
                snprintf(msg, 1024, "lsm_tree:open config expects {write_buffer, <positive int>} but \"%s\" is not a valid positive integer between 8KB and 512MB in bytes", o);
                return make_error_msg(env, msg);
            }
            if (n < (8 * KB) || n > (512 * MB)) n = (8 * KB);
            rc = lsm_config(db, op, &n);
            return rc != LSM_OK ? make_error(env, rc) : 0;
        } else if (option[0] == ATOM_PAGE_SIZE && op == LSM_CONFIG_PAGE_SIZE) {
            if (!enif_get_int(env, option[1], &n) || n < 512 || n > (8 * KB)) {
                enif_get_string(env, option[1], o, sizeof o, ERL_NIF_LATIN1);
                snprintf(msg, 1024, "lsm_tree:open config expects {page_size, <positive int>} but \"%s\" is not a valid positive integer between 512 bytes and 8KB in bytes", o);
                return make_error_msg(env, msg);
            }
            if (n < 512 || n > (8 * KB)) n = (8 * KB);
            rc = lsm_config(db, op, &n);
            return rc != LSM_OK ? make_error(env, rc) : 0;
        } else if (option[0] == ATOM_BLOCK_SIZE && op == LSM_CONFIG_BLOCK_SIZE) {
            if (!enif_get_int(env, option[1], &n) || n < (8 * KB) || n > (512 * MB)) {
                enif_get_string(env, option[1], o, sizeof o, ERL_NIF_LATIN1);
                snprintf(msg, 1024, "lsm_tree:open config expects {block_size, <positive int>} but \"%s\" is not a valid positive integer between 8KB and 512MB in bytes", o);
                return make_error_msg(env, msg);
            }
            if (n < (8 * KB) || n > (512 * MB)) n = (8 * KB);
            rc = lsm_config(db, op, &n);
            return rc != LSM_OK ? make_error(env, rc) : 0;
        } else if (option[0] == ATOM_LOG_SIZE && op == LSM_CONFIG_LOG_SIZE) {
            if (!enif_get_int(env, option[1], &n) || n < (8 * KB) || n > (512 * MB)) {
                enif_get_string(env, option[1], o, sizeof o, ERL_NIF_LATIN1);
                snprintf(msg, 1024, "lsm_tree:open config expects {log_size, <positive int>} but \"%s\" is not a valid positive integer between 8KB and 512MB in bytes", o);
                return make_error_msg(env, msg);
            }
            if (n < (8 * KB) || n > (512 * MB)) n = (8 * KB);
            rc = lsm_config(db, op, &n);
            return rc != LSM_OK ? make_error(env, rc) : 0;
        } else if (option[0] == ATOM_SAFETY && op == LSM_CONFIG_SAFETY) {
            if (option[1] == ATOM_OFF || option[1] == ATOM_FALSE) {
                n = 0;
                rc = lsm_config(db, op, &n);
                return rc != LSM_OK ? make_error(env, rc) : 0;
            } else if (option[1] == ATOM_NORMAL) {
                n = 1;
                rc = lsm_config(db, op, &n);
                return rc != LSM_OK ? make_error(env, rc) : 0;
            } else if (option[1] == ATOM_FULL) {
                n = 2;
                rc = lsm_config(db, op, &n);
                return rc != LSM_OK ? make_error(env, rc) : 0;
            } else {
                enif_get_string(env, option[1], o, sizeof o, ERL_NIF_LATIN1);
                snprintf(msg, 1024, "lsm_tree:open config expects {safety, <on | off | true | false>} but \"%s\" is not a valid setting", o);
                return make_error_msg(env, msg);
            }
        } else if (option[0] == ATOM_AUTOWORK && op == LSM_CONFIG_AUTOWORK) {
            if (option[1] == ATOM_ON || option[1] == ATOM_TRUE) {
                n = 1;
                rc = lsm_config(db, op, &n);
                return rc != LSM_OK ? make_error(env, rc) : 0;
            } else if (option[1] == ATOM_OFF || option[1] == ATOM_FALSE) {
                n = 0;
                rc = lsm_config(db, op, &n);
                return rc != LSM_OK ? make_error(env, rc) : 0;
            } else {
                enif_get_string(env, option[1], o, sizeof o, ERL_NIF_LATIN1);
                snprintf(msg, 1024, "lsm_tree:open config expects {autowork, <on | off | true | false>} but \"%s\" is not a valid setting", o);
                return make_error_msg(env, msg);
            }
        } else if (option[0] == ATOM_MMAP && op == LSM_CONFIG_MMAP) {
            if (option[1] == ATOM_ON || option[1] == ATOM_TRUE) {
                n = 1;
                rc = lsm_config(db, op, &n);
                return rc != LSM_OK ? make_error(env, rc) : 0;
            } else if (option[1] == ATOM_OFF || option[1] == ATOM_FALSE) {
                n = 0;
                rc = lsm_config(db, op, &n);
                return rc != LSM_OK ? make_error(env, rc) : 0;
            } else {
                enif_get_string(env, option[1], o, sizeof o, ERL_NIF_LATIN1);
                snprintf(msg, 1024, "lsm_tree:open config expects {mmap, <on | off | true | false>} but \"%s\" is not a valid setting", o);
                return make_error_msg(env, msg);
            }
        } else if (option[0] == ATOM_USE_LOG && op == LSM_CONFIG_USE_LOG) {
            if (option[1] == ATOM_ON || option[1] == ATOM_TRUE) {
                n = 1;
                rc = lsm_config(db, op, &n);
                return rc != LSM_OK ? make_error(env, rc) : 0;
            } else if (option[1] == ATOM_OFF || option[1] == ATOM_FALSE) {
                n = 0;
                rc = lsm_config(db, op, &n);
                return rc != LSM_OK ? make_error(env, rc) : 0;
            } else {
                enif_get_string(env, option[1], o, sizeof o, ERL_NIF_LATIN1);
                snprintf(msg, 1024, "lsm_tree:open config expects {use_log, <on | off | true | false>} but \"%s\" is not a valid setting", o);
                return make_error_msg(env, msg);
            }
        } else if (option[0] == ATOM_NMERGE && op == LSM_CONFIG_NMERGE) {
            if (!enif_get_int(env, option[1], &n)) {
                enif_get_string(env, option[1], o, sizeof o, ERL_NIF_LATIN1);
                snprintf(msg, 1024, "lsm_tree:open config expects {nmerge, <positive int>} but \"%s\" is not a valid positive integer", o);
                return make_error_msg(env, msg);
            }
            if (n < 4 || n > 100) n = 4;
            rc = lsm_config(db, op, &n);
            return rc != LSM_OK ? make_error(env, rc) : 0;
        } else if (option[0] == ATOM_CREATE_IF_MISSING || option[0] == ATOM_CREATE ||
                   option[0] == ATOM_ERROR_IF_EXISTS ||
                   option[0] == ATOM_CHECKSUM_VALUES) {
            list = tail; continue; // Skip these legal values
        } if (head == tail) {
            enif_get_string(env, option[0], o, sizeof o, ERL_NIF_LATIN1);
            snprintf(msg, 1024, "lsm_tree:open \"%s\" is not a valid option", o);
            return make_error_msg(env, msg);
        } else {
            list = tail;
        }
    }
    return 0; // 0 means the requested argument
}

//-spec open(string(), open_options()) -> {ok, tree()} | {error, term()}.
static ERL_NIF_TERM lsm_tree_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char filename[MAXPATHLEN];

    if (!(enif_get_string(env, argv[0], filename, sizeof filename, ERL_NIF_LATIN1) &&
          enif_is_list(env, argv[1])))
      return ATOM_BADARG;

    // Get the options
    //TODO: deal with other open options... int flags = __get_file_open_flags(env, argv[1]);

    LsmTreeHandle* tree_handle = enif_alloc_resource(lsm_tree_RESOURCE, sizeof(LsmTreeHandle));
    if (!tree_handle) return ATOM_ENOMEM;

    lsm_db* db;
    int rc = lsm_new(&erl_nif_env, &db);
    if (rc != LSM_OK) return make_error(env, rc);

    int opts[] = {LSM_CONFIG_WRITE_BUFFER, LSM_CONFIG_PAGE_SIZE, LSM_CONFIG_BLOCK_SIZE,
                  LSM_CONFIG_LOG_SIZE,     LSM_CONFIG_SAFETY,    LSM_CONFIG_AUTOWORK,
                  LSM_CONFIG_MMAP,         LSM_CONFIG_USE_LOG,   LSM_CONFIG_NMERGE};
    for (int i = 0; i < sizeof(opts); i++) {
        ERL_NIF_TERM t = __config_lsm_env(env, argv[1], opts[i], db);
        if (t != 0) {
            enif_release_resource(tree_handle);
            return t;
        }
    }

    rc = lsm_open(db, filename);
    if (rc != LSM_OK) {
      // TODO: automate recovery
      // if (rc == LSM_CORRUPT) {
      //     recover database if options says to...
      enif_release_resource(tree_handle);
      return make_error(env, rc);
    }

    tree_handle->pDb = db;
    ERL_NIF_TERM result = enif_make_resource(env, tree_handle);
    enif_release_resource(tree_handle);
    return enif_make_tuple2(env, ATOM_OK, result);
}

//-spec close(tree()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_tree_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int rc = LSM_OK;
    LsmTreeHandle* tree_handle;

    if (!enif_get_resource(env, argv[0], lsm_tree_RESOURCE, (void**)&tree_handle))
      return ATOM_BADARG;

    /* NOTE: All open cursors must be closed first, or this will fail! */
    rc = lsm_close(tree_handle->pDb);
    return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
}

//-spec get(tree(), key()) -> {ok, value()} | not_found | {error, term()}.
static ERL_NIF_TERM lsm_tree_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmTreeHandle* tree_handle;
    if (enif_get_resource(env, argv[0], lsm_tree_RESOURCE, (void**)&tree_handle))
    {
        ErlNifBinary key;
        if (!enif_inspect_binary(env, argv[1], &key))
            return ATOM_BADARG;

        int rc = LSM_OK;
        lsm_db* db = tree_handle->pDb;
        lsm_cursor* cursor = 0;
        rc = lsm_csr_open(db, &cursor);
        if (rc != LSM_OK) return make_error(env, rc);
        rc = lsm_csr_seek(cursor, key.data, key.size, LSM_SEEK_EQ);
        if (rc == LSM_OK) {
            if (lsm_csr_valid(cursor) == LSM_NOTFOUND) return ATOM_NOTFOUND;
            void *raw_value;
            int raw_value_size;
            rc = lsm_csr_value(cursor, &raw_value, &raw_value_size);
            if (rc != LSM_OK) return make_error(env, rc);
            rc = lsm_csr_close(cursor);
            if (rc != LSM_OK) return make_error(env, rc);
            ERL_NIF_TERM value;
            unsigned char* bin = enif_make_new_binary(env, raw_value_size, &value);
            if (!bin) return ATOM_ENOMEM;
            memcpy(bin, raw_value, raw_value_size);
            return enif_make_tuple2(env, ATOM_OK, value);
        } else {
            return make_error(env, rc);
        }
    }
    return ATOM_BADARG;
}

//-spec put(tree(), key(), value()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_tree_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmTreeHandle* tree_handle;
    if (enif_get_resource(env, argv[0], lsm_tree_RESOURCE, (void**)&tree_handle))
    {
        ErlNifBinary key, value;
        if (!(enif_inspect_binary(env, argv[1], &key) &&
              enif_inspect_binary(env, argv[2], &value)))
          return ATOM_BADARG;

        int rc = LSM_OK;
        lsm_db* db = tree_handle->pDb;
        rc = lsm_write(db, (void *)key.data, key.size, (void *)value.data, value.size);
        return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
    }
    return ATOM_BADARG;
}

//-spec delete(tree(), key()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_tree_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmTreeHandle* tree_handle;
    if (enif_get_resource(env, argv[0], lsm_tree_RESOURCE, (void**)&tree_handle))
    {
        ErlNifBinary key;
        if (!enif_inspect_binary(env, argv[1], &key))
          return ATOM_BADARG;

        int rc = LSM_OK;
        lsm_db* db = tree_handle->pDb;
        rc = lsm_delete(db, key.data, key.size);
        return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
    }
    return ATOM_BADARG;
}

typedef enum {
    LSM_OP_SALVAGE = 1,
    LSM_OP_FLUSH,
    LSM_OP_CHECKPOINT,
    LSM_OP_TRUNCATE,
    LSM_OP_COMPACT,
    LSM_OP_DESTROY,
    LSM_OP_UPGRADE,
    LSM_OP_VERIFY,
} lsm_worker_ops;

static ERL_NIF_TERM __op_worker(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], lsm_worker_ops op)
{
    LsmTreeHandle* tree_handle;
    ErlNifBinary config;

    if (enif_get_resource(env, argv[0], lsm_tree_RESOURCE, (void**)&tree_handle) &&
        enif_inspect_binary(env, argv[1], &config))
    {
        int rc = LSM_OK;
        lsm_db* db = tree_handle->pDb;

        switch (op)
          {
          // Run recovery on a corrupt database in hopes of returning it to a usable state.
          case LSM_OP_SALVAGE: //TODO
            break;
          // Attempt to flush the contents of the in-memory tree to disk.
          case LSM_OP_FLUSH:
            rc = lsm_work(db, LSM_WORK_FLUSH, 0, 0);
            return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
          // Write a checkpoint (if one exists in memory) to the database file.
          case LSM_OP_CHECKPOINT:
            rc = lsm_work(db, LSM_WORK_CHECKPOINT, 0, 0);
            return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
          // Empties an open database of all key/value pairs, may not reduce on-disk files.
          case LSM_OP_TRUNCATE: //TODO
            break;
          // Runs the merge worker process to compact on disk files.
          case LSM_OP_COMPACT: //TODO
            rc = lsm_work(db, LSM_WORK_OPTIMIZE, 0, 0);
            return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
            break;
          // Close the database and delete all files on disk, handle is invalid after this.
          case LSM_OP_DESTROY: //TODO
            break;
          // Upgrades on-disk files from one version's format to the next.
          case LSM_OP_UPGRADE: //TODO
            break;
          // Verifies the integrity of the files on disk as consistent.
          case LSM_OP_VERIFY: //TODO
            break;
          default:; /* FALLTHRU */
          }
        return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
    }
    return ATOM_BADARG;
}

//-spec truncate(tree()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_tree_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __op_worker(env, argc, argv, LSM_OP_TRUNCATE); }

//-spec verify(tree()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_tree_verify(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __op_worker(env, argc, argv, LSM_OP_VERIFY); }

//-spec salvage(string()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_tree_salvage(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __op_worker(env, argc, argv, LSM_OP_SALVAGE); }

//-spec flush(tree()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_tree_flush(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __op_worker(env, argc, argv, LSM_OP_FLUSH); }

//-spec checkpoint(tree()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_tree_checkpoint(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __op_worker(env, argc, argv, LSM_OP_CHECKPOINT); }

//-spec compact(tree()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_tree_compact(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __op_worker(env, argc, argv, LSM_OP_COMPACT); }

//-spec destroy(tree()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_tree_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __op_worker(env, argc, argv, LSM_OP_TRUNCATE); }

//-spec upgrade(tree()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_tree_upgrade(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __op_worker(env, argc, argv, LSM_OP_UPGRADE); }

//-spec cursor_open(tree()) -> {ok, cursor()} | {error, term()}.
static ERL_NIF_TERM lsm_cursor_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmTreeHandle* tree_handle;
    if (enif_get_resource(env, argv[0], lsm_tree_RESOURCE, (void**)&tree_handle))
    {
        int rc = LSM_OK;
        lsm_db* db = tree_handle->pDb;
        LsmCursorHandle* cursor_handle = enif_alloc_resource(lsm_cursor_RESOURCE, sizeof(LsmCursorHandle));
        enif_release_resource(cursor_handle);
        if (cursor_handle == 0) return ATOM_ENOMEM;
        lsm_cursor* cursor = cursor_handle->pCsr;
        rc = lsm_csr_open(db, &cursor);
        if (rc != LSM_OK) return make_error(env, rc);
        else {
          ERL_NIF_TERM result = enif_make_resource(env, cursor_handle);
          return enif_make_tuple2(env, ATOM_OK, result);
        }
    }
    return ATOM_BADARG;
}

//-spec cursor_close(cursor()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_cursor_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmCursorHandle *cursor_handle;
    if (enif_get_resource(env, argv[0], lsm_cursor_RESOURCE, (void**)&cursor_handle)) {
        lsm_cursor* cursor = cursor_handle->pCsr;
        int rc = lsm_csr_close(cursor);
        return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
    }
    return ATOM_BADARG;
}

//-spec cursor_position(cursor(), key()) -> ok | {ok, value()} | {error, term()}.
static ERL_NIF_TERM lsm_cursor_position(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmCursorHandle *cursor_handle;
    if (enif_get_resource(env, argv[0], lsm_cursor_RESOURCE, (void**)&cursor_handle))
    {
        ErlNifBinary key;
        if (!enif_inspect_binary(env, argv[1], &key))
            return ATOM_BADARG;

        int rc = LSM_OK;
        lsm_cursor* cursor = cursor_handle->pCsr;
        rc = lsm_csr_seek(cursor, key.data, key.size, LSM_SEEK_EQ); //TODO support _GE and _LE
        if (rc == LSM_OK) {
            if (lsm_csr_valid(cursor) == LSM_NOTFOUND) {
                return ATOM_NOTFOUND;
            } else {
                void *raw_key;
                int raw_size;
                rc = lsm_csr_key(cursor, &raw_key, &raw_size);
                if (rc != LSM_OK) {
                    return make_error(env, rc);
                } else {
                    if (raw_size != key.size || __compare_keys(raw_key, raw_size, key.data, key.size)) {
                        // Not an exact EQ match, return partially matching key we found.
                        ERL_NIF_TERM partial_key;
                        unsigned char* bin = enif_make_new_binary(env, raw_size, &partial_key);
                        if (!bin) return ATOM_ENOMEM;
                        memcpy(bin, raw_key, raw_size);
                        return enif_make_tuple2(env, ATOM_OK, partial_key);
                    } else {
                        return ATOM_OK;
                    }
                }
            }
        } else {
            return make_error(env, rc);
        }
    }
    return ATOM_BADARG;
}

static ERL_NIF_TERM __cursor_key_ret(ErlNifEnv* env, lsm_cursor *cursor, int rc)
{
    if (rc == LSM_OK) {
        void *raw_key;
        int raw_key_size;
        rc = lsm_csr_key(cursor, &raw_key, &raw_key_size);
        if (rc == LSM_OK) {
            ERL_NIF_TERM key;
            unsigned char* bin = enif_make_new_binary(env, raw_key_size, &key);
            if (!bin) return ATOM_ENOMEM;
            memcpy(bin, raw_key, raw_key_size);
            return enif_make_tuple2(env, ATOM_OK, key);
        }
    }
    return make_error(env, rc);
}

static ERL_NIF_TERM __cursor_kv_ret(ErlNifEnv* env, lsm_cursor *cursor, int rc)
{
    if (rc == LSM_OK) {
        void *raw_key, *raw_value;
        int raw_key_size, raw_value_size;
        rc = lsm_csr_key(cursor, &raw_key, &raw_key_size);
        if (rc == LSM_OK) {
            ERL_NIF_TERM key;
            unsigned char* kbin = enif_make_new_binary(env, raw_key_size, &key);
            if (!kbin) return ATOM_ENOMEM;
            rc = lsm_csr_value(cursor, &raw_value, &raw_value_size);
            if (rc == LSM_OK) {
                ERL_NIF_TERM value;
                unsigned char* vbin = enif_make_new_binary(env, raw_value_size, &value);
                if (!vbin) return ATOM_ENOMEM;
                memcpy(kbin, raw_key, raw_key_size);
                memcpy(vbin, raw_value, raw_value_size);
                return enif_make_tuple3(env, ATOM_OK, key, value);
            }
        }
    }
    return make_error(env, rc);
}

static ERL_NIF_TERM __cursor_value_ret(ErlNifEnv* env, lsm_cursor *cursor, int rc)
{
    if (rc == LSM_OK) {
        void *raw_value;
        int raw_value_size;
        rc = lsm_csr_value(cursor, &raw_value, &raw_value_size);
        if (rc == LSM_OK) {
            ERL_NIF_TERM value;
            unsigned char* bin = enif_make_new_binary(env, raw_value_size, &value);
            if (!bin) return ATOM_ENOMEM;
            memcpy(bin, raw_value, raw_value_size);
            return enif_make_tuple2(env, ATOM_OK, value);
        }
    }
    return make_error(env, rc);
}

typedef enum {
    LSM_DIR_NEXT = 0,
    LSM_DIR_PREV,
} lsm_dir;

static ERL_NIF_TERM __cursor_np_worker(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], CursorRetFun cursor_ret, lsm_dir direction)
{
    LsmCursorHandle *cursor_handle;
    if (enif_get_resource(env, argv[0], lsm_cursor_RESOURCE, (void**)&cursor_handle)) {
        lsm_cursor* cursor = cursor_handle->pCsr;
        return cursor_ret(env, cursor, direction == LSM_DIR_NEXT ? lsm_csr_next(cursor) : lsm_csr_prev(cursor));
    }
    return ATOM_BADARG;
}

//-spec cursor_next(cursor()) -> {ok, key(), value()} | not_found | {error, term()}.
static ERL_NIF_TERM lsm_cursor_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __cursor_np_worker(env, argc, argv, __cursor_kv_ret, LSM_DIR_NEXT); }

//-spec cursor_next_key(cursor()) -> {ok, key()} | not_found | {error, term()}.
static ERL_NIF_TERM lsm_cursor_next_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __cursor_np_worker(env, argc, argv, __cursor_key_ret, LSM_DIR_NEXT); }

//-spec cursor_next_value(cursor()) -> {ok, value()} | not_found | {error, term()}.
static ERL_NIF_TERM lsm_cursor_next_value(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __cursor_np_worker(env, argc, argv, __cursor_value_ret, LSM_DIR_NEXT); }

//-spec cursor_prev(cursor()) -> {ok, key(), value()} | not_found | {error, term()}.
static ERL_NIF_TERM lsm_cursor_prev(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __cursor_np_worker(env, argc, argv, __cursor_kv_ret, LSM_DIR_PREV); }

//-spec cursor_prev_key(cursor()) -> {ok, key()} | not_found | {error, term()}.
static ERL_NIF_TERM lsm_cursor_prev_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __cursor_np_worker(env, argc, argv, __cursor_key_ret, LSM_DIR_PREV); }

//-spec cursor_prev_value(cursor()) -> {ok, value()} | not_found | {error, term()}.
static ERL_NIF_TERM lsm_cursor_prev_value(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __cursor_np_worker(env, argc, argv, __cursor_value_ret, LSM_DIR_PREV); }

//-spec cursor_first(cursor()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_cursor_first(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmCursorHandle *cursor_handle;
    if (enif_get_resource(env, argv[0], lsm_cursor_RESOURCE, (void**)&cursor_handle)) {
        lsm_cursor* cursor = cursor_handle->pCsr;
        int rc = lsm_csr_first(cursor);
        return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
    }
    return ATOM_BADARG;
}

//-spec cursor_last(cursor()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_cursor_last(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmCursorHandle *cursor_handle;
    if (enif_get_resource(env, argv[0], lsm_cursor_RESOURCE, (void**)&cursor_handle)) {
        lsm_cursor* cursor = cursor_handle->pCsr;
        int rc = lsm_csr_last(cursor);
        return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
    }
    return ATOM_BADARG;
}

#if 0
//TODO txn suport
static ERL_NIF_TERM lsm_txn_begin(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmTreeHandle* tree_handle;
    if (enif_get_resource(env, argv[0], lsm_tree_RESOURCE, (void**)&tree_handle))
    {
        int rc = LSM_OK;
        lsm_db* db = tree_handle->pDb;
        LsmCursorHandle* cursor_handle = enif_alloc_resource(lsm_cursor_RESOURCE, sizeof(LsmCursorHandle));
        if (cursor_handle == 0) return ATOM_ENOMEM;
        lsm_cursor* cursor = cursor_handle->pCsr;
        if (cursor == 0) return ATOM_ENOMEM;
        rc = lsm_csr_open(db, &cursor);
        if (rc != LSM_OK) {
          enif_release_resource(cursor_handle);
          return make_error(env, rc);
        } else {
          ERL_NIF_TERM result = enif_make_resource(env, cursor_handle);
          enif_release_resource(cursor_handle);
          return enif_make_tuple2(env, ATOM_OK, result);
        }
    }
    return ATOM_BADARG;
}

static ERL_NIF_TERM lsm_txn_commit(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
}

static ERL_NIF_TERM lsm_txn_abort(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
}

static ERL_NIF_TERM lsm_snapshot??(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
}

static ERL_NIF_TERM lsm_stats??(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    lsm_info();
}
#endif

static void* __malloc(lsm_env* env, int bytes) { return enif_alloc((size_t)bytes); }
static void* __realloc(lsm_env* env, void* p, int bytes) { return enif_realloc(p, (size_t)bytes); }
static void __free(lsm_env* env, void* p) { enif_free(p); }

static int __mutex_static(lsm_env* env, int t, lsm_mutex** m)
{
  /* TODO: what to do about "static" mutexes?
  switch(t)
    {
    case LSM_MUTEX_GLOBAL:
      break;
    case LSM_MUTEX_HEAP:
      break;
    default:
      assert(-1);
    }
  */
  *m = (lsm_mutex*)enif_mutex_create("lsm_tree mutex");
  return m ? LSM_OK : LSM_ERROR;
}
static int __mutex_create(lsm_env* env, lsm_mutex** m)
{
  *m = (lsm_mutex*)enif_mutex_create("lsm_tree mutex");
  return m ? LSM_OK : LSM_ERROR;
}
static void __mutex_destroy(lsm_mutex* m) { enif_mutex_destroy((ErlNifMutex*)m); }
static void __mutex_lock(lsm_mutex* m) { enif_mutex_lock((ErlNifMutex*)m); }
static int __mutex_trylock(lsm_mutex* m) { return enif_mutex_trylock((ErlNifMutex*)m); }
static void __mutex_unlock(lsm_mutex *m) { return enif_mutex_unlock((ErlNifMutex*)m); }
#ifdef LSM_DEBUG
static int __mutex_held(lsm_mutex* m)
{
  ErlNifMutex *p = (ErlNifMutex *)m;
  return p ? enif_thread_equal(p->owner, pthread_self()) : 1;
}
static int __mutex_not_held(lsm_mutex* m)
{
  ErlNifMutex *p = (ErlNifMutex *)m;
  return p ? !enif_thread_equal(p->owner, pthread_self()) : 1;
}
#endif

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;

    // Use Erlang/BEAM's memory allocation and mutex functions.
    memcpy(&erl_nif_env, lsm_default_env(), sizeof(lsm_env));

#if 0 // TODO
    erl_nif_env.xMalloc = __malloc;                 /* malloc(3) function */
    erl_nif_env.xRealloc = __realloc;               /* realloc(3) function */
    erl_nif_env.xFree = __free;                     /* free(3) function */
    erl_nif_env.xMutexStatic = __mutex_static;      /* Obtain a static mutex */
    erl_nif_env.xMutexNew = __mutex_create;         /* Get a new dynamic mutex */
    erl_nif_env.xMutexDel = __mutex_destroy;        /* Delete an allocated mutex */
    erl_nif_env.xMutexEnter = __mutex_lock;         /* Grab a mutex */
    erl_nif_env.xMutexTry = __mutex_trylock;        /* Attempt to obtain a mutex */
    erl_nif_env.xMutexLeave = __mutex_unlock;       /* Leave a mutex */
#endif
#ifdef LSM_DEBUG
    erl_nif_env.xMutexHeld = __mutex_held;          /* Return true if mutex is held */
    erl_nif_env.xMutexNotHeld = __mutex_not_held;   /* Return true if mutex not held */
#endif
#ifdef MISSING_FROM_LSM_API
    // Set the key comparison function
    erl_nif_env.xCmp = __compare_keys;
#else
#endif
    // TODO: pass log messages up to lager: lsm_config_log();
    // TODO: what does the lsm_config_work_hook(); do that might be useful... dunno yet.
    lsm_tree_RESOURCE = enif_open_resource_type(env, NULL, "lsm_tree_resource", NULL, flags, NULL);
    lsm_cursor_RESOURCE = enif_open_resource_type(env, NULL, "lsm_cursor_resource", NULL, flags, NULL);
    ATOM_ERROR = make_atom(env, "error");
    ATOM_OK = make_atom(env, "ok");
    ATOM_NOTFOUND = make_atom(env, "not_found");
    ATOM_ENOMEM = make_atom(env, "enomem");
    ATOM_BADARG = make_atom(env, "badarg");
    ATOM_WRITE_BUFFER = make_atom(env, "write_buffer");
    ATOM_PAGE_SIZE = make_atom(env, "page_size");
    ATOM_BLOCK_SIZE = make_atom(env, "block_size");
    ATOM_LOG_SIZE = make_atom(env, "log_size");
    ATOM_SAFETY = make_atom(env, "safety");
    ATOM_AUTOWORK = make_atom(env, "autowork");
    ATOM_MMAP = make_atom(env, "mmap");
    ATOM_USE_LOG = make_atom(env, "use_log");
    ATOM_NMERGE = make_atom(env, "nmerge");
    ATOM_ON = make_atom(env, "on");
    ATOM_OFF = make_atom(env, "off");
    ATOM_NORMAL = make_atom(env, "normal");
    ATOM_FULL = make_atom(env, "full");
    ATOM_TRUE = make_atom(env, "true");
    ATOM_FALSE = make_atom(env, "false");
    ATOM_CREATE = make_atom(env, "create");
    ATOM_CREATE_IF_MISSING = make_atom(env, "create_if_missing");
    ATOM_ERROR_IF_EXISTS = make_atom(env, "error_if_exists");
    ATOM_CHECKSUM_VALUES =make_atom(env, "checksum_values");
    return 0;
}

ERL_NIF_INIT(lsm_tree, nif_funcs, &on_load, NULL, NULL, NULL);
