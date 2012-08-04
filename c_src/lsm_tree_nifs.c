/*
 * lsm_tree: A Riak/KV backend using SQLite4's Log-Structured Merge Tree
 *
 * Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
 *
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

#include "erl_nif.h"
#include "erl_driver.h"

#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <sys/param.h>
#include <sys/errno.h>

#include "lsm.h"

#define KB 1024
#define MB (1024 * KB)

#define lsm_csr_invalid(__cursor) (lsm_csr_valid(__cursor) == 0)

static ErlNifResourceType* lsm_tree_RESOURCE;
static ErlNifResourceType* lsm_cursor_RESOURCE;
static lsm_env erl_nif_env;

typedef struct {
    lsm_db *pDb;                       /* LSM database handle */
    lsm_cursor* pSharedCsr;            /* for use in get() and delete() */
} LsmTreeHandle;

typedef struct {
    lsm_cursor *pCsr;                  /* LSM cursor handle */
    LsmTreeHandle *tree_handle;
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
static ERL_NIF_TERM ATOM_PUT;
static ERL_NIF_TERM ATOM_DELETE;
static ERL_NIF_TERM ATOM_CREATE;            // Shorthand for CREATE_IF_MISSING below
static ERL_NIF_TERM ATOM_CREATE_IF_MISSING; //TODO, not yet implemented.
static ERL_NIF_TERM ATOM_ERROR_IF_EXISTS;   //TODO, not yet implemented.
static ERL_NIF_TERM ATOM_CHECKSUM_VALUES;   //TODO, not yet implemented.

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
static ERL_NIF_TERM lsm_cursor_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_transact(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_salvage(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_flush(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_checkpoint(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_compact(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_upgrade(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lsm_tree_verify(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

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
    {"cursor_delete",     1, lsm_cursor_delete},
    {"transact",          2, lsm_transact},
};


static ERL_NIF_TERM make_atom(ErlNifEnv* env, const char* name)
{
    ERL_NIF_TERM ret;
    if(enif_make_existing_atom(env, name, &ret, ERL_NIF_LATIN1))
      return ret;
    return enif_make_atom(env, name);
}

#if defined(TEST) || defined(DEBUG)
#define make_error_msg(__env, __msg) __make_error_msg(__env, __msg, __FILE__, __LINE__)
static ERL_NIF_TERM __make_error_msg(ErlNifEnv* env, const char* msg, char *file, int line)
#else
#define make_error_msg(__env, __msg) __make_error_msg(__env, __msg)
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

static const char *__ioerr_msg(int err)
{
    char *s, *t;
    int len = 1024;
    static char buf[1024];

    s = erl_errno_id(err);
    if (strcmp(s, "unknown") == 0 && err == EOVERFLOW) {
        s = "EOVERFLOW";
    }

    for (t = buf; *s && --len; s++, t++) {
        *t = tolower(*s);
    }
    *t = '\0';
    return (const char*)buf;
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
    case LSM_BUSY:           return __error_msg(env, "lsm_busy", file, line);
    case LSM_NOMEM:          return ATOM_ENOMEM;
    case LSM_IOERR:          return __error_msg(env, __ioerr_msg(rc), file, line);
    case LSM_CORRUPT:        return __error_msg(env, "lsm_corrupt", file, line);
    case LSM_FULL:           return __error_msg(env, "lsm_full", file, line);
    case LSM_CANTOPEN:       return __error_msg(env, "lsm_cant_open", file, line);
    case LSM_MISUSE:         return __error_msg(env, "lsm_misuse", file, line);
    case LSM_ERROR: default:;/* FALLTHRU */
    }
  return __error_msg(env, "lsm_error", __FILE__, __LINE__);
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
    memset(tree_handle, 0, sizeof(LsmTreeHandle));

    lsm_db* db;
    int rc = lsm_new(&erl_nif_env, &db);
    if (rc != LSM_OK) return make_error(env, rc);

    int opts[] = {LSM_CONFIG_WRITE_BUFFER, LSM_CONFIG_PAGE_SIZE, LSM_CONFIG_BLOCK_SIZE,
                  LSM_CONFIG_LOG_SIZE,     LSM_CONFIG_SAFETY,    LSM_CONFIG_AUTOWORK,
                  LSM_CONFIG_MMAP,         LSM_CONFIG_USE_LOG,   LSM_CONFIG_NMERGE};
    int i;
    for (i = 0; i < sizeof(opts); i++) {
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

    lsm_db* db = tree_handle->pDb;

    /* Rollback any uncommitted write transactions */
    lsm_rollback(db, 2);

    /* Close the shared cursor */
    lsm_cursor* cursor = tree_handle->pSharedCsr;
    if (cursor)
        lsm_csr_close(cursor);

    /* At this point all open cursors should be closed, otherwise this will fail (misuse) */
    rc = lsm_close(db);
    return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
}

static int __shared_cursor(ErlNifEnv* env, LsmTreeHandle* tree_handle)
{
    if (tree_handle->pSharedCsr)
        return LSM_OK;

    lsm_db* db = tree_handle->pDb;
    int rc = LSM_OK;
    rc = lsm_csr_open(db, &tree_handle->pSharedCsr);
    if (rc != LSM_OK) return make_error(env, rc);
    rc = lsm_begin(db, 1);
    if (rc != LSM_OK) {
        lsm_csr_close(tree_handle->pSharedCsr);
        tree_handle->pSharedCsr = 0;
        return make_error(env, rc);
    }
    return LSM_OK;
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
        rc = __shared_cursor(env, tree_handle);
        if (rc != LSM_OK) return make_error(env, rc);
        lsm_cursor* cursor = tree_handle->pSharedCsr;

        rc = lsm_csr_seek(cursor, key.data, key.size, LSM_SEEK_EQ);
        if (rc == LSM_OK) {
            if (lsm_csr_invalid(cursor)) return ATOM_NOTFOUND;
            void *raw_value;
            int raw_value_size;
            rc = lsm_csr_value(cursor, &raw_value, &raw_value_size);
            if (rc != LSM_OK) {
                lsm_csr_close(cursor);
                tree_handle->pSharedCsr = 0;
                return make_error(env, rc);
            }
            ERL_NIF_TERM value;
            unsigned char* bin = enif_make_new_binary(env, raw_value_size, &value);
            if (!bin) return ATOM_ENOMEM;
            memcpy(bin, raw_value, raw_value_size);
            return enif_make_tuple2(env, ATOM_OK, value);
        } else {
            lsm_csr_close(cursor);
            tree_handle->pSharedCsr = 0;
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

//        rc = lsm_begin(db, 2); DPRINTF("lsm_tree_put/lsm_begin() = %d\n", rc);
//        if (rc != LSM_OK) return make_error(env, rc);

        rc = lsm_write(db, key.data, key.size, value.data, value.size);
        return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
//        if (rc == LSM_OK) {
//            int trc = lsm_commit(db, 2); DPRINTF("lsm_tree_put/lsm_commit() = %d/%d\n", rc, trc);
//            return LSM_OK;
//        } else {
//            int trc = lsm_rollback(db, 2); DPRINTF("lsm_tree_put/lsm_rollback() = %d/%d\n", rc, trc);
//            return make_error(env, rc);
//        }
    }
    return ATOM_BADARG;
}

//-spec delete(tree(), key()) -> ok | not_found | {error, term()}.
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
        rc = __shared_cursor(env, tree_handle);
        if (rc != LSM_OK) return make_error(env, rc);
        lsm_cursor* cursor = tree_handle->pSharedCsr;

        rc = lsm_csr_seek(cursor, key.data, key.size, LSM_SEEK_EQ);
        if (rc == LSM_OK) {
            if (lsm_csr_invalid(cursor)) return ATOM_NOTFOUND;
            void *raw_key;
            int raw_key_size;
            rc = lsm_csr_key(cursor, &raw_key, &raw_key_size);
            if (rc != LSM_OK) {
                lsm_rollback(db, 2);
                lsm_csr_close(cursor);
                return make_error(env, rc);
            }

            rc = lsm_begin(db, 2);
            if (rc != LSM_OK) return make_error(env, rc);

            rc = lsm_delete(db, raw_key, raw_key_size);
            if (rc == LSM_OK) {
                lsm_commit(db, 2);
                return LSM_OK;
            } else {
                lsm_rollback(db, 2);
                lsm_csr_close(cursor);
                tree_handle->pSharedCsr = 0;
                return make_error(env, rc);
            }
            return ATOM_OK;
        } else {
            return make_error(env, rc);
        }
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

    if (enif_get_resource(env, argv[0], lsm_tree_RESOURCE, (void**)&tree_handle)) {
        int rc = LSM_OK;
        lsm_db* db = tree_handle->pDb;

        switch (op) {
        case LSM_OP_SALVAGE: //TODO
            // Run recovery on a corrupt database in hopes of returning it to a usable state.
            break;

          case LSM_OP_FLUSH:
              // Attempt to flush the contents of the in-memory tree to disk.
              rc = lsm_work(db, LSM_WORK_FLUSH, 0, 0);
              return rc == LSM_OK ? ATOM_OK : make_error(env, rc);

        case LSM_OP_CHECKPOINT:
            // Write a checkpoint (if one exists in memory) to the database file.
            rc = lsm_work(db, LSM_WORK_CHECKPOINT, 0, 0);
            return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
        case LSM_OP_TRUNCATE: //TODO
            // Empties an open database of all key/value pairs, may not reduce on-disk files.
            break;
        case LSM_OP_COMPACT: //TODO
            // Runs the merge worker process to compact on disk files.
            rc = lsm_work(db, LSM_WORK_OPTIMIZE, 0, 0);
            return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
            break;
        case LSM_OP_DESTROY: //TODO
            // Close the database and delete all files on disk, handle is invalid after this.
            break;
        case LSM_OP_UPGRADE: //TODO
            // Upgrades on-disk files from one version's format to the next.
            break;
        case LSM_OP_VERIFY: //TODO
            // Verifies the integrity of the files on disk as consistent.
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
        if (cursor_handle == 0) return ATOM_ENOMEM;
        memset(cursor_handle, 0, sizeof(LsmCursorHandle));
        cursor_handle->tree_handle = tree_handle;
        lsm_cursor* cursor;

        rc = lsm_begin(db, 1);
        if (rc != LSM_OK) return  make_error(env, rc);

        rc = lsm_csr_open(db, &cursor);
        if (rc != LSM_OK) return make_error(env, rc);
        cursor_handle->pCsr = cursor;
        ERL_NIF_TERM result = enif_make_resource(env, cursor_handle);
        enif_keep_resource(tree_handle);
        enif_release_resource(cursor_handle);
        return enif_make_tuple2(env, ATOM_OK, result);
    }
    return ATOM_BADARG;
}

//-spec cursor_close(cursor()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_cursor_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmCursorHandle *cursor_handle;
    if (enif_get_resource(env, argv[0], lsm_cursor_RESOURCE, (void**)&cursor_handle)) {
        enif_release_resource(cursor_handle->tree_handle);
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
            if (lsm_csr_invalid(cursor)) return ATOM_NOTFOUND;
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
        } else {
            return make_error(env, rc);
        }
    }
    return ATOM_BADARG;
}

static ERL_NIF_TERM __cursor_key_ret(ErlNifEnv* env, lsm_cursor *cursor, int rc)
{
    if (lsm_csr_invalid(cursor)) return ATOM_NOTFOUND;
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
    if (lsm_csr_invalid(cursor)) return ATOM_NOTFOUND;
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
    if (lsm_csr_invalid(cursor)) return ATOM_NOTFOUND;
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
        int rc = LSM_OK;
        ERL_NIF_TERM result = cursor_ret(env, cursor, rc);
        if (rc == LSM_OK) {
            if (direction == LSM_DIR_NEXT)
                rc = lsm_csr_next(cursor);
            else
                rc = lsm_csr_prev(cursor);
            return rc == LSM_OK ? result : make_error(env, rc);
        } else if (rc == LSM_MISUSE) {
            if (lsm_csr_invalid(cursor))
                return ATOM_BADARG;
        }
        return make_error(env, rc);
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

//-spec cursor_delete(cursor()) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_cursor_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmCursorHandle *cursor_handle;
    if (enif_get_resource(env, argv[0], lsm_cursor_RESOURCE, (void**)&cursor_handle)) {
        lsm_cursor* cursor = cursor_handle->pCsr;
        void* raw_key;
        int raw_key_size;
        int rc = lsm_csr_key(cursor, &raw_key, &raw_key_size);
        if (rc == LSM_OK) {
            lsm_db* db = cursor_handle->tree_handle->pDb;
            rc = lsm_delete(db, raw_key, raw_key_size);
        } else {
            return make_error(env, rc);
        }
        return rc == LSM_OK ? ATOM_OK : make_error(env, rc);
    }
    return ATOM_BADARG;
}

// -spec transact(tree(), [transact_spec()]) -> ok | {error, term()}.
static ERL_NIF_TERM lsm_transact(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    LsmTreeHandle *tree_handle;
    ERL_NIF_TERM head, tail, list;
    const ERL_NIF_TERM* array;
    int rc = LSM_OK;

    if (!(enif_get_resource(env, argv[0], lsm_tree_RESOURCE, (void**)&tree_handle) && argc == 2))
        return ATOM_BADARG;

    lsm_db* db = tree_handle->pDb;

    // We'll use the shared cursor to position, then delete keys
    rc = __shared_cursor(env, tree_handle);
    if (rc != LSM_OK) return make_error(env, rc);
    lsm_cursor* cursor = tree_handle->pSharedCsr;

    // put/delete/trasact calls are serialized so they all operate within the "2"
    // transaction isolation level.
    rc = lsm_begin(db, 2);
    if (rc != LSM_OK) return make_error(env, rc);

    // Now, simply iterate over the list of operations, perform them atomically.
    //-type transact_spec() :: {put, key(), value()} | {delete, value()}.
    list = argv[1];
    while (enif_get_list_cell(env, list, &head, &tail)) {
        static char msg[1024], o[1024];
        ErlNifBinary key, value;
        int arity;

        if (!enif_get_tuple(env, head, &arity, &array)) {
          if (array[0] == ATOM_PUT && arity == 3) {
              rc = enif_inspect_binary(env, array[1], &key);
              if (!rc) goto err;
              rc = enif_inspect_binary(env, array[2], &value);
              if (!rc) goto err;
              rc = lsm_write(db, key.data, key.size, value.data, value.size);
              if (rc != LSM_OK) goto err;
          } else if (array[0] == ATOM_DELETE && arity == 2) {
              rc = enif_inspect_binary(env, array[1], &key);
              if (!rc) goto err;
              rc = lsm_csr_seek(cursor, key.data, key.size, LSM_SEEK_EQ);
              if (rc == LSM_OK) {
                // Silently ignore deletes for non-existent keys.
                if (lsm_csr_invalid(cursor))
                  continue;
                void *raw_key;
                int raw_key_size;
                rc = lsm_csr_key(cursor, &raw_key, &raw_key_size);
                if (rc != LSM_OK) goto err;
                rc = lsm_delete(db, raw_key, raw_key_size);
                if (rc != LSM_OK) goto err;
              } // else... Again, silently ignore deletes for non-existent keys.
          } else {
              enif_get_atom(env, head, o, sizeof o, ERL_NIF_LATIN1);
              snprintf(msg, 1024, "lsm_tree:transact \"%s\" is not a valid tuple", o);
              lsm_csr_close(cursor);
              tree_handle->pSharedCsr = 0;
              lsm_rollback(db, 2);
              return make_error_msg(env, msg); // TODO should goto err...
              goto err;
          }
        }
    }
    lsm_commit(db, 2);
    return LSM_OK;
 err:
    lsm_csr_close(cursor);
    tree_handle->pSharedCsr = 0;
    lsm_rollback(db, 2);
    return make_error(env, rc);
}

#if 0 //TODO
static ERL_NIF_TERM lsm_snapshot(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
}

static ERL_NIF_TERM lsm_stats(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    lsm_info();
}
#endif

static void* __malloc(lsm_env* env, int bytes) { return enif_alloc((size_t)bytes); }
static void* __realloc(lsm_env* env, void* p, int bytes) { return enif_realloc(p, (size_t)bytes); }
static void __free(lsm_env* env, void* p) { enif_free(p); }

#if 0
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
#endif
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

    erl_nif_env.xMalloc = __malloc;                 /* malloc(3) function */
    erl_nif_env.xRealloc = __realloc;               /* realloc(3) function */
    erl_nif_env.xFree = __free;                     /* free(3) function */
#if 0 // TODO
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
    ATOM_PUT = make_atom(env, "put");
    ATOM_DELETE = make_atom(env, "delete");
    ATOM_CREATE = make_atom(env, "create");
    ATOM_CREATE_IF_MISSING = make_atom(env, "create_if_missing");
    ATOM_ERROR_IF_EXISTS = make_atom(env, "error_if_exists");
    ATOM_CHECKSUM_VALUES =make_atom(env, "checksum_values");
    return 0;
}

ERL_NIF_INIT(lsm_tree, nif_funcs, &on_load, NULL, NULL, NULL);
