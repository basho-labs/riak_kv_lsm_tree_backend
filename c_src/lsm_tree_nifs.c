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

static ErlNifResourceType* lsm_tree_RESOURCE;
static ErlNifResourceType* lsm_cursor_RESOURCE;
static lsm_env* lsm_env_RESOURCE;

typedef struct {
  lsm_db *pDb;                       /* LSM database handle */
} LsmTreeHandle;

typedef lsm_cursor *LsmCursorHandle; /* LSM cursor handle */

// Atoms (initialized in on_load)
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_NOTFOUND;
static ERL_NIF_TERM ATOM_ENOMEM;
static ERL_NIF_TERM ATOM_BADARG;

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
static ERL_NIF_TERM lsm_txn_begin(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_txn_commit(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_txn_abort(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_salvage(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_compact(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_drop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_upgrade(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_verify(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_count(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

static ErlNifFunc nif_funcs[] =
{
    {"open",              2, lsm_tree_open},
    {"close",             1, lsm_tree_close},
    {"get",               2, lsm_tree_get},
    {"put",               3, lsm_tree_put},
    {"delete",            2, lsm_tree_delete},
    {"salvage",           2, lsm_tree_salvage},
    {"sync",              2, lsm_tree_sync},
    {"truncate",          2, lsm_tree_truncate},
    {"compact",           2, lsm_tree_compact},
    {"drop",              2, lsm_tree_drop},
    {"upgrade",           2, lsm_tree_upgrade},
    {"verify",            2, lsm_tree_verify},
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
    {"txn_begin",         2, lsm_txn_begin},
    {"txn_commit",        2, lsm_txn_commit},
    {"txn_abort",         2, lsm_txn_abort},
};


static inline ERL_NIF_TERM make_atom(ErlNifEnv* env, const char* name)
{
  ERL_NIF_TERM ret;
  if(enif_make_existing_atom(env, name, &ret, ERL_NIF_LATIN1))
    return ret;
  return enif_make_atom(env, name);
}

static inline ERL_NIF_TERM make_ok(ErlNifEnv* env, ERL_NIF_TERM mesg)
{
  return enif_make_tuple2(env, ATOM_OK, mesg);
}

static inline ERL_NIF_TERM make_error_msg(ErlNifEnv* env, const char* mesg)
{
  return enif_make_tuple2(env, ATOM_ERROR, make_atom(env, mesg));
}

static inline ERL_NIF_TERM make_error(ErlNifEnv* env, int rc)
{
  switch(rc)
    {
    case LSM_OK:             return ATOM_OK;
    case LSM_NOTFOUND:       return ATOM_NOTFOUND;
    case LSM_BUSY:           return make_error(env, "lsm_busy");
    case LSM_NOMEM:          return ATOM_ENOMEM;
    case LSM_IOERR:          return make_error(env, "lsm_ioerr");
    case LSM_CORRUPT:        return make_error(env, "lsm_corrupt");
    case LSM_FULL:           return make_error(env, "lsm_full");
    case LSM_CANTOPEN:       return make_error(env, "lsm_cant_open");
    case LSM_MISUSE:         return make_error(env, "lsm_misuse");
    case LSM_ERROR:          /* FALLTHRU */
    default:                 /* FALLTHRU */
    }
  return make_error(env, "lsm_error");
}

static ERL_NIF_TERM lsm_tree_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary config;
    char filename[MAXPATHLEN];

    if (!(enif_get_string(env, argv[0], filename, sizeof filename, ERL_NIF_LATIN1) &&
          enif_inspect_binary(env, argv[1], &config)))
      return ATOM_BADARG;

    LsmTreeHandle* tree_handle = enif_alloc_resource(lsm_tree_RESOURCE, sizeof(LsmTreeHandle));
    if (!tree_handle) return ATOM_ENOMEM;

    int rc = lsm_new(0, &tree_handle->pDb);
    if (rc != LSM_OK) return make_error(env, rc);

    rc = lsm_open(tree_handle->pDb, filename);
    if (rc != LSM_OK) {
      // TODO: automate recovery
      // if (rc == LSM_CORRUPT) {
      //     recover database if config says to...
      lsm_close(tree_handle->pDb);
      enif_free_resource(tree_handle);
      return make_error(env, rc);
    }

    ERL_NIF_TERM result = enif_make_resource(env, tree_handle);
    enif_keep_resource(tree_handle);
    enif_release_resource(tree_handle);
    return enif_make_tuple2(env, ATOM_OK, result);
}

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
        lsm_cursor* cursor = (lsm_cursor*)enif_alloc(lsm_cursor, sizeof(lsm_cursor*));
        if (cursor == 0) return ATOM_ENOMEM;
        rc = lsm_csr_open(db, &cursor);
        if (rc != LSM_OK) {
          enif_free(cursor);
          return make_error(env, rc);
        }
        rc = lsm_csr_seek(cursor, key.data, key.size, LSM_SEEK_EQ);
        if (rc == LSM_OK) {
          if (lsm_csr_valid(cursor) == LSM_NOTFOUND) {
            return ATOM_NOTFOUND;
          } else {
            void *raw_value;
            int raw_size;
            rc = lsm_csr_value(cursor, &raw_value, &raw_size);
            if (rc != LSM_OK) {
              lsm_csr_close(cursor);
              enif_free(cursor);
              return make_error(rc);
            } else {
              ERL_NIF_TERM value;
              unsigned char* bin = enif_make_new_binary(env, raw_size, &value);
              memcpy(bin, raw_value, raw_value);
              cursor->close(cursor);
              return enif_make_tuple2(env, ATOM_OK, value);
            }
          }
        } else {
          enif_free(cursor);
          return make_error(rc);
        }
    }
    return ATOM_BADARG;
}

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
        rc = lsm_write(db, (void *)key.data, key.size, (void *)data.data, data.size);
        return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
    }
    return ATOM_BADARG;
}

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
    LSM_OP_SYNC,
    LSM_OP_TRUNCATE,
    LSM_OP_COMPACT,
    LSM_OP_DROP,
    LSM_OP_UPGRADE,
    LSM_OP_VERIFY,
} lsm_worker_ops;

static inline ERL_NIF_TERM __op_worker(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], lsm_worker_ops op)
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
          // Flushes any state in-memory to disk ensuring a consistent state.
          case LSM_OP_SYNC:
            rc = lsm_work(db, LSM_WORK_CHECKPOINT, 0, 0);
            return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
          // Empties an open database of all key/value pairs, may not reduce on-disk files.
          case LSM_OP_TRUNCATE: //TODO
            break;
          // Runs the merge worker process to compact on disk files.
          case LSM_OP_COMPACT: //TODO
            rc = lsm_work(db, LSM_WORK_OPTIMIZE | LSM_WORK_MERGE, 0, 0);
            return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
            break;
          // Close the database and delete all files on disk, handle is invalid after this.
          case LSM_OP_DROP: //TODO
            break;
          // Upgrades on-disk files from one version's format to the next.
          case LSM_OP_UPGRADE: //TODO
            break;
          // Verifies the integrity of the files on disk as consistent.
          case LSM_OP_VERIFY: //TODO
            break;
          default:
            /* FALLTHRU */
          }
        return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
    }
    return ATOM_BADARG;
}

static ERL_NIF_TERM lsm_tree_salvage(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __op_worker(env, argc, argv, LSM_OP_SALVAGE); }
static ERL_NIF_TERM lsm_tree_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __op_worker(env, argc, argv, LSM_OP_SYNC); }
static ERL_NIF_TERM lsm_tree_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __op_worker(env, argc, argv, LSM_OP_TRUNCATE); }
static ERL_NIF_TERM lsm_tree_compact(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __op_worker(env, argc, argv, LSM_OP_COMPACT); }
static ERL_NIF_TERM lsm_tree_drop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __op_worker(env, argc, argv, LSM_OP_TRUNCATE); }
static ERL_NIF_TERM lsm_tree_upgrade(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __op_worker(env, argc, argv, LSM_OP_UPGRADE); }
static ERL_NIF_TERM lsm_tree_verify(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __op_worker(env, argc, argv, LSM_OP_VERIFY); }

static ERL_NIF_TERM lsm_cursor_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmTreeHandle* tree_handle;
    if (enif_get_resource(env, argv[0], lsm_tree_RESOURCE, (void**)&tree_handle))
    {
        int rc = LSM_OK;
        lsm_db* db = tree_handle->pDb;
        LsmCursorHandle* cursor_handle = enif_alloc_resource(lsm_cursor_RESOURCE, sizeof(LsmCursorHandle));
        lsm_cursor* cursor = (lsm_cursor*)cursor_handle;
        if (cursor == 0) return ATOM_ENOMEM;
        rc = lsm_csr_open(db, &cursor);
        if (rc != LSM_OK) {
          enif_free_resource(cursor_handle);
          return make_error(env, rc);
        } else {
          ERL_NIF_TERM result = enif_make_resource(env, cursor_handle);
          enif_keep_resource(tree_handle);
          enif_release_resource(cursor_handle);
          return enif_make_tuple2(env, ATOM_OK, result);
        }
    }
    return ATOM_BADARG;
}

static ERL_NIF_TERM lsm_cursor_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmCursorHandle *cursor_handle;
    if (enif_get_resource(env, argv[0], lsm_cursor_RESOURCE, (void**)&cursor_handle))
    {
        lsm_cursor* cursor = (lsm_cursor*)cursor_handle;
        int rc = lsm_csr_close(cursor);
        return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
    }
    return ATOM_BADARG;
}

static ERL_NIF_TERM lsm_cursor_position(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmCursorHandle *cursor_handle;
    if (enif_get_resource(env, argv[0], lsm_cursor_RESOURCE, (void**)&cursor_handle))
    {
        ErlNifBinary key;
        if (!enif_inspect_binary(env, argv[1], &key))
          return ATOM_BADARG;

        int rc = LSM_OK;
        lsm_cursor* cursor = (lsm_cursor*)cursor_handle;
        if (cursor == 0) return ATOM_ENOMEM;
        rc = lsm_csr_seek(cursor, key.data, key.size, LSM_SEEK_EQ);
        if (rc == LSM_OK) {
          if (lsm_csr_valid(cursor) == LSM_NOTFOUND) {
            return ATOM_NOTFOUND;
          } else {
            void *raw_key;
            int raw_size;
            rc = lsm_csr_key(cursor, &raw_key, &raw_size);
            if (rc != LSM_OK) {
              return make_error(rc);
            } else {
              if (raw_size != key.size || __lsm_key_compare(raw_key, raw_size, key.data, key.size)) {
                ERL_NIF_TERM key;
                unsigned char* bin = enif_make_new_binary(env, raw_size, &key);
                if (!bin) {
                  return ATOM_ENOMEM;
                } else {
                  memcpy(bin, raw_key, raw_size);
                  return enif_make_tuple2(env, ATOM_OK, bin);
                }
              } else {
                return make_ok;
              }
            }
          }
        } else {
          return make_error(rc);
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
            ERL_NIF_TERM key, value;
            unsigned char* kbin = enif_make_new_binary(env, raw_key_size, &key);
            if (!kbin) return ATOM_ENOMEM;
            unsigned char* vbin = enif_make_new_binary(env, raw_value_size, &value);
            if (!vbin) {
              enif_free_binary(kbin);
              return ATOM_ENOMEM;
            } else {
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
        lsm_cursor* cursor = (lsm_cursor *)cursor_handle;
        return cursor_ret(env, cursor, direction == LSM_DIR_NEXT ? cursor->next(cursor) : cursor->prev(cursor));
    }
    return ATOM_BADARG;
}

static ERL_NIF_TERM lsm_cursor_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __cursor_np_worker(env, argc, argv, __cursor_kv_ret, LSM_DIR_NEXT); }

static ERL_NIF_TERM lsm_cursor_next_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __cursor_np_worker(env, argc, argv, __cursor_key_ret, LSM_DIR_NEXT); }

static ERL_NIF_TERM lsm_cursor_next_value(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __cursor_np_worker(env, argc, argv, __cursor_value_ret, LSM_DIR_NEXT); }

static ERL_NIF_TERM lsm_cursor_prev(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __cursor_np_worker(env, argc, argv, __cursor_kv_ret, LSM_DIR_PREV); }

static ERL_NIF_TERM lsm_cursor_prev_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __cursor_np_worker(env, argc, argv, __cursor_key_ret, LSM_DIR_PREV); }

static ERL_NIF_TERM lsm_cursor_prev_value(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{ return __cursor_np_worker(env, argc, argv, __cursor_value_ret, LSM_DIR_PREV); }

static ERL_NIF_TERM lsm_cursor_first(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmCursorHandle *cursor_handle;
    if (enif_get_resource(env, argv[0], lsm_cursor_RESOURCE, (void**)&cursor_handle)) {
        lsm_cursor* cursor = (lsm_cursor *)cursor_handle;
        int rc = lsm_csr_first(cursor);
        return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
    }
    return ATOM_BADARG;
}

static ERL_NIF_TERM lsm_cursor_last(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmCursorHandle *cursor_handle;
    if (enif_get_resource(env, argv[0], lsm_cursor_RESOURCE, (void**)&cursor_handle)) {
        lsm_cursor* cursor = (lsm_cursor *)cursor_handle;
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
        lsm_cursor* cursor = (lsm_cursor*)cursor_handle;
        if (cursor == 0) return ATOM_ENOMEM;
        rc = lsm_csr_open(db, &cursor);
        if (rc != LSM_OK) {
          enif_free_resource(cursor_handle);
          return make_error(env, rc);
        } else {
          ERL_NIF_TERM result = enif_make_resource(env, cursor_handle);
          enif_keep_resource(tree_handle);
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
static void* __realloc(lsm_env* env, void* p, int bytes) { return enif_relloc(p, (size_t)bytes); }
static void* __free(lsm_env* env, void* p) { return enif_free(p); }

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
  m = (lsm_mutex*)enif_mutex_create("lsm_tree mutex");
  return m ? LSM_OK : LSM_ERROR;
}
static int __mutex_create(lsm_env* env, lsm_mutex* m)
{
  m = (lsm_mutex*)enif_mutex_create("lsm_tree mutex");
  return m ? LSM_OK : LSM_ERROR;
}
static void __mutex_destroy(lsm_mutex* m) { enif_mutex_destroy((ErlNifMutex*)lsm_mutex); }
static void __mutex_lock(lsm_mutex* m) { enif_mutex_lock((ErlNifMutex*)lsm_mutex); }
static int __mutex_trylock(lsm_mutex* m) { return enif_mutex_trylock((ErlNifMutex*)lsm_mutex); }
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

static int __compare_keys(const void *key1, int n1, const void *key2, int n2)
{
    int c = memcmp(key1, key2, ((n1 < n2) ? n1 : n2));
    if (c == 0)
      c = n1 - n2;
    return c;
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;
    lsm_env_RESOUCE = lsm_default_env();
    // Use Erlang/BEAM's memory allocation and mutex functions.
    lsm_env_RESOURCE->xMalloc = __malloc;                 /* malloc(3) function */
    lsm_env_RESOURCE->xRealloc = __realloc;               /* realloc(3) function */
    lsm_env_RESOURCE->xFree = __free;                     /* free(3) function */
    lsm_env_RESOURCE->xMutexStatic = __mutex_static;      /* Obtain a static mutex */
    lsm_env_RESOURCE->xMutex = __mutex_create;            /* Get a new dynamic mutex */
    lsm_env_RESOURCE->xMutexDel = __mutex_destroy;        /* Delete an allocated mutex */
    lsm_env_RESOURCE->xMutexEnter = __mutex_lock;         /* Grab a mutex */
    lsm_env_RESOURCE->xMutexTry = __mutex_trylock;        /* Attempt to obtain a mutex */
    lsm_env_RESOURCE->xMutexLeave = __mutex_unlock;       /* Leave a mutex */
#ifdef LSM_DEBUG
    lsm_env_RESOURCE->xMutexHeld = __mutex_held;          /* Return true if mutex is held */
    lsm_env_RESOURCE->xMutexNotHeld = __mutex_not_held;   /* Return true if mutex not held */
#endif
    // Set the key comparison function
    lsm_env_RESOURCE->xCmp = __compare_keys;
    // TODO: pass log messages up to lager: lsm_config_log();
    // TODO: what does the lsm_config_work_hook(); do that might be useful... dunno yet.
    lsm_tree_RESOURCE = enif_open_resource_type(env, NULL, "lsm_tree_resource", NULL, flags, NULL);
    lsm_cursor_RESOURCE = enif_open_resource_type(env, NULL, "lsm_cursor_resource", NULL, flags, NULL);
    ATOM_ERROR = make_atom(env, "error");
    ATOM_OK = make_atom(env, "ok");
    ATOM_NOTFOUND = make_atom(env, "not_found");
    ATOM_ENOMEM = make_atom(env, "enomem");
    ATOM_BADARG = make_atom(env, "badarg");
    return 0;
}

ERL_NIF_INIT(lsm_tree, nif_funcs, &on_load, NULL, NULL, NULL);
