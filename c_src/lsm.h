/*
** 2011-08-10
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file defines the LSM API.
*/
#ifndef _LSM_H
#define _LSM_H
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
** Opaque handle types.
*/
typedef struct lsm_db lsm_db;               /* Database connection handle */
typedef struct lsm_cursor lsm_cursor;       /* Database cursor handle */
typedef struct lsm_mutex lsm_mutex;         /* Mutex handle */
typedef struct lsm_file lsm_file;           /* OS file handle */

/* 64-bit integer type used for file offsets. */
typedef long long int lsm_i64;              /* 64-bit signed integer type */

/* Forward reference */
typedef struct lsm_env lsm_env;             /* Runtime environment */

/*
** Run-time environment used by LSM
*/
struct lsm_env {
  int nByte;                 /* Size of this structure in bytes */
  int iVersion;              /* Version number of this structure */
  /****** file i/o ***********************************************/
  void *pVfsCtx;
  int (*xFullpath)(lsm_env*, const char *, char *, int *);
  int (*xOpen)(lsm_env*, const char *, lsm_file **);
  int (*xRead)(lsm_file *, lsm_i64, void *, int);
  int (*xWrite)(lsm_file *, lsm_i64, void *, int);
  int (*xTruncate)(lsm_file *, lsm_i64);
  int (*xSync)(lsm_file *);
  int (*xSectorSize)(lsm_file *);
  int (*xRemap)(lsm_file *, lsm_i64, void **, lsm_i64*);
  int (*xFileid)(lsm_file *, void *pBuf, int *pnBuf);
  int (*xClose)(lsm_file *);
  int (*xUnlink)(lsm_env*, const char *);
  /****** memory allocation ****************************************/
  void *pMemCtx;
  void *(*xMalloc)(lsm_env*, int);            /* malloc(3) function */
  void *(*xRealloc)(lsm_env*, void *, int);   /* realloc(3) function */
  void (*xFree)(lsm_env*, void *);            /* free(3) function */
  /****** mutexes ****************************************************/
  void *pMutexCtx;
  int (*xMutexStatic)(lsm_env*,int,lsm_mutex**); /* Obtain a static mutex */
  int (*xMutexNew)(lsm_env*, lsm_mutex**);       /* Get a new dynamic mutex */
  void (*xMutexDel)(lsm_mutex *);           /* Delete an allocated mutex */
  void (*xMutexEnter)(lsm_mutex *);         /* Grab a mutex */
  int (*xMutexTry)(lsm_mutex *);            /* Attempt to obtain a mutex */
  void (*xMutexLeave)(lsm_mutex *);         /* Leave a mutex */
  int (*xMutexHeld)(lsm_mutex *);           /* Return true if mutex is held */
  int (*xMutexNotHeld)(lsm_mutex *);        /* Return true if mutex not held */
  /* New fields may be added in future releases, in which case the
  ** iVersion value will increase. */
};

/* 
** Values that may be passed as the second argument to xMutexStatic. 
*/
#define LSM_MUTEX_GLOBAL 1
#define LSM_MUTEX_HEAP   2

/*
** Return a pointer to the default LSM run-time environment
*/
lsm_env *lsm_default_env(void);

/*
** Error codes.
*/
#define LSM_OK         0
#define LSM_ERROR      1
#define LSM_BUSY       5
#define LSM_NOMEM      7
#define LSM_IOERR     10
#define LSM_CORRUPT   11
#define LSM_FULL      13
#define LSM_CANTOPEN  14
#define LSM_MISUSE    21

/* 
** Open and close a connection to a named database.
*/
int lsm_new(lsm_env*, lsm_db **ppDb);
int lsm_open(lsm_db *pDb, const char *zFilename);
int lsm_close(lsm_db *pDb);

/*
** Return a pointer to the environment used by the database connection 
** passed as the first argument. Assuming the argument is valid, this 
** function always returns a valid environment pointer - it cannot fail.
*/
lsm_env *lsm_get_env(lsm_db *pDb);

/*
** Configure a database connection.
*/
int lsm_config(lsm_db *, int, ...);

/*
** The following values may be passed as the second argument to lsm_config().
**
**   LSM_CONFIG_WRITE_BUFFER
**     A read/write integer parameter. This value determines the maximum amount
**     of space (in bytes) used to accumulate writes in main-memory before 
**     they are flushed to a level 0 segment.
**
**   LSM_CONFIG_PAGE_SIZE
**     A read/write integer parameter. This parameter may only be set before
**     lsm_open() has been called.
**
**   LSM_CONFIG_BLOCK_SIZE
**     A read/write integer parameter. This parameter may only be set before
**     lsm_open() has been called.
**
**   LSM_CONFIG_LOG_SIZE
**     A read/write integer parameter.
**
**   LSM_CONFIG_SAFETY
**     A read/write integer parameter. Valid values are 0, 1 (the default) 
**     and 2. This parameter determines how robust the database is in the
**     face of a system crash (e.g. a power failure or operating system 
**     crash). As follows:
**
**       0 (off):    No robustness. A system crash may corrupt the database.
**
**       1 (normal): Some robustness. A system crash may not corrupt the
**                   database file, but recently committed transactions may
**                   be lost following recovery.
**
**       2 (full):   Full robustness. A system crash may not corrupt the
**                   database file. Following recovery the database file
**                   contains all successfully committed transactions.
**
**   LSM_CONFIG_AUTOWORK
**     A read/write integer parameter.
**
**     TODO: This should configure some kind of threshold for turning on 
**     auto-work. Right now it is Boolean - 1 for on and 0 for off. Default 1.
**
**   LSM_CONFIG_MMAP
**     A read/write integer parameter. True to use mmap() to access the 
**     database file. False otherwise.
**
**   LSM_CONFIG_USE_LOG
**     A read/write boolean parameter. True (the default) to use the log
**     file normally. False otherwise.
**
**   LSM_CONFIG_NMERGE
**     A read/write integer parameter. The minimum number of segments to
**     merge together at a time. Default value 4.
*/
#define LSM_CONFIG_WRITE_BUFFER  1
#define LSM_CONFIG_PAGE_SIZE     2
#define LSM_CONFIG_SAFETY        3
#define LSM_CONFIG_BLOCK_SIZE    4
#define LSM_CONFIG_AUTOWORK      5
#define LSM_CONFIG_LOG_SIZE      6
#define LSM_CONFIG_MMAP          7
#define LSM_CONFIG_USE_LOG       8
#define LSM_CONFIG_NMERGE        9

#define LSM_SAFETY_OFF    0
#define LSM_SAFETY_NORMAL 1
#define LSM_SAFETY_FULL   2


/*
** Invoke the memory allocation functions that belong to environment
** pEnv. Or the system defaults if no memory allocation functions have 
** been registered.
*/
void *lsm_malloc(lsm_env*, size_t);
void *lsm_realloc(lsm_env*, void *, size_t);
void lsm_free(lsm_env*, void *);

/*
** Configure a callback to which debugging and other messages should 
** be directed. Only useful for debugging lsm.
*/
void lsm_config_log(lsm_db *, void (*)(void *, int, const char *), void *);

/*
** Configure a callback that is invoked if the database connection ever
** writes to the database file.
*/
void lsm_config_work_hook(lsm_db *, void (*)(lsm_db *, void *), void *);

/*
** Query a database connection for operational statistics or data.
*/
int lsm_info(lsm_db *, int, ...);

/*
** The following values may be passed as the second argument to lsm_info().
**
**   LSM_INFO_NWRITE
**     The third parameter should be of type (int *). The location pointed
**     to by the third parameter is set to the number of 4KB pages written to
**     the database file during the lifetime of this connection. 
**
**   LSM_INFO_NREAD
**     The third parameter should be of type (int *). The location pointed
**     to by the third parameter is set to the number of 4KB pages read from
**     the database file during the lifetime of this connection.
**
**   LSM_INFO_DB_STRUCTURE
**     The third argument should be of type (char **). The location pointed
**     to is populated with a pointer to a nul-terminated string containing
**     the string representation of a Tcl data-structure reflecting the 
**     current structure of the database file. Specifically, the current state
**     of the worker snapshot. The returned string should be eventually freed 
**     by the caller using lsm_free().
**
**     The returned list contains one element for each level in the database,
**     in order from most to least recent. Each element contains a 
**     single element for each segment comprising the corresponding level,
**     starting with the lhs segment, then each of the rhs segments (if any)
**     in order from most to least recent.
**
**     Each segment element is itself a list of 6 integer values, as follows:
**
**        1. First page of segment
**        2. Last page of segment
**        3. Root page of segment (if applicable).
**        4. Total number of pages in segment.
**
**   LSM_INFO_ARRAY_STRUCTURE
**     There should be two arguments passed following this option (i.e. a 
**     total of four arguments passed to lsm_info()). The first argument 
**     should be the page number of the first page in a database array 
**     (perhaps obtained from an earlier INFO_DB_STRUCTURE call). The second 
**     trailing argument should be of type (char **). The location pointed 
**     to is populated with a pointer to a nul-terminated string that must 
**     be eventually freed using lsm_free() by the caller.
**
**     The output string contains the text representation of a Tcl list of
**     integers. Each pair of integers represent a range of pages used by
**     the identified array. For example, if the array occupies database
**     pages 993 to 1024, then pages 2048 to 2777, then the returned string
**     will be "993 1024 2048 2777".
**
**     If the specified integer argument does not correspond to the first
**     page of any database array, LSM_ERROR is returned and the output
**     pointer is set to a NULL value.
**
**   LSM_INFO_PAGE_ASCII_DUMP
**     As with LSM_INFO_ARRAY_STRUCTURE, there should be two arguments passed
**     with calls that specify this option - an integer page number and a
**     (char **) used to return a nul-terminated string that must be later
**     freed using lsm_free(). In this case the output string is populated
**     with a human-readable description of the page content.
**
**     If the page cannot be decoded, it is not an error. In this case the
**     human-readable output message will report the systems failure to 
**     interpret the page data.
**
**   LSM_INFO_PAGE_HEX_DUMP
**     This argument is similar to PAGE_ASCII_DUMP, except that keys and
**     values are represented using hexadecimal notation instead of ascii.
**
**   LSM_INFO_LOG_STRUCTURE
**     The third argument should be of type (char **). The location pointed
**     to is populated with a pointer to a nul-terminated string containing
**     the string representation of a Tcl data-structure. The returned 
**     string should be eventually freed by the caller using lsm_free().
**
**     The Tcl structure returned is a list of six integers that describe
**     the current structure of the log file.
*/
#define LSM_INFO_NWRITE           1
#define LSM_INFO_NREAD            2
#define LSM_INFO_DB_STRUCTURE     3
#define LSM_INFO_LOG_STRUCTURE    4
#define LSM_INFO_ARRAY_STRUCTURE  5
#define LSM_INFO_PAGE_ASCII_DUMP  6
#define LSM_INFO_PAGE_HEX_DUMP    7


/* 
** Open and close transactions and nested transactions.
**
** lsm_begin():
**   Used to open transactions and sub-transactions. A successful call to 
**   lsm_begin() ensures that there are at least iLevel nested transactions 
**   open. To open a top-level transaction, pass iLevel==1. To open a 
**   sub-transaction within the top-level transaction, iLevel==2. Passing 
**   iLevel==0 is a no-op.
**
** lsm_commit():
**   Used to commit transactions and sub-transactions. A successful call 
**   to lsm_commit() ensures that there are at most iLevel nested 
**   transactions open. To commit a top-level transaction, pass iLevel==0. 
**   To commit all sub-transactions inside the main transaction, pass
**   iLevel==1.
**
** lsm_rollback():
**   Used to roll back transactions and sub-transactions. A successful call 
**   to lsm_rollback() restores the database to the state it was in when
**   the iLevel'th nested sub-transaction (if any) was first opened. And then
**   closes transactions to ensure that there are at most iLevel nested
**   transactions open.
**
**   Passing iLevel==0 rolls back and closes the top-level transaction.
**   iLevel==1 also rolls back the top-level transaction, but leaves it
**   open. iLevel==2 rolls back the sub-transaction nested directly inside
**   the top-level transaction (and leaves it open).
*/
int lsm_begin(lsm_db *pDb, int iLevel);
int lsm_commit(lsm_db *pDb, int iLevel);
int lsm_rollback(lsm_db *pDb, int iLevel);

/* 
** Write a new value into the database. If a value with a duplicate key 
** already exists it is replaced.
*/
int lsm_write(lsm_db *pDb, void *pKey, int nKey, void *pVal, int nVal);

/*
** Delete a value from the database. No error is returned if the specified
** key value does not exist in the database.
*/
int lsm_delete(lsm_db *pDb, void *pKey, int nKey);

/*
** This function is called by a thread to work on the database structure.
** The actual operations performed by this function depend on the value 
** passed as the "flags" parameter:
**
** LSM_WORK_FLUSH:
**   Attempt to flush the contents of the in-memory tree to disk.
**
** LSM_WORK_CHECKPOINT:
**   Write a checkpoint (if one exists in memory) to the database file.
**
** LSM_WORK_OPTIMIZE:
**   This flag is only regcognized if LSM_WORK_MERGE is also set.
*/
int lsm_work(lsm_db *pDb, int flags, int nPage, int *pnWrite);

#define LSM_WORK_FLUSH           0x00000001
#define LSM_WORK_CHECKPOINT      0x00000002
#define LSM_WORK_OPTIMIZE        0x00000004

/* 
** Open and close a database cursor.
*/
int lsm_csr_open(lsm_db *pDb, lsm_cursor **ppCsr);
int lsm_csr_close(lsm_cursor *pCsr);

/* 
** If the fourth parameter is LSM_SEEK_EQ, LSM_SEEK_GE or LSM_SEEK_LE,
** this function searches the database for an entry with key (pKey/nKey). 
** If an error occurs, an LSM error code is returned. Otherwise, LSM_OK.
**
** If no error occurs and the requested key is present in the database, the
** cursor is left pointing to the entry with the specified key. Or, if the 
** specified key is not present in the database the state of the cursor 
** depends on the value passed as the final parameter, as follows:
**
**   LSM_SEEK_EQ:
**     The cursor is left at EOF (invalidated). A call to lsm_csr_valid()
**     returns non-zero.
**
**   LSM_SEEK_LE:
**     The cursor is left pointing to the largest key in the database that
**     is smaller than (pKey/nKey). If the database contains no keys smaller
**     than (pKey/nKey), the cursor is left at EOF.
**
**   LSM_SEEK_GE:
**     The cursor is left pointing to the smallest key in the database that
**     is larger than (pKey/nKey). If the database contains no keys larger
**     than (pKey/nKey), the cursor is left at EOF.
**
** If the fourth parameter is LSM_SEEK_LEFAST, this function searches the
** database in a similar manner to LSM_SEEK_LE, with two differences:
**
**   1) Even if a key can be found (the cursor is not left at EOF), the
**      lsm_csr_value() function may not be used (attempts to do so return
**      LSM_MISUSE).
**
**   2) The key that the cursor is left pointing to may be one that has 
**      been recently deleted from the database. In this case it is guaranteed
**      that the returned key is larger than any key currently in the database
**      that is less than or equal to (pKey/nKey).
**
** LSM_SEEK_LEFAST requests are intended to be used to allocate database
** keys.
*/
int lsm_csr_seek(lsm_cursor *pCsr, void *pKey, int nKey, int eSeek);

/*
** Values that may be passed as the fourth argument to lsm_csr_seek().
*/
#define LSM_SEEK_LEFAST   -2
#define LSM_SEEK_LE       -1
#define LSM_SEEK_EQ        0
#define LSM_SEEK_GE        1

int lsm_csr_first(lsm_cursor *pCsr);
int lsm_csr_last(lsm_cursor *pCsr);

/*
** Advance the specified cursor to the next or previous key in the database.
** Return LSM_OK if successful, or an LSM error code otherwise.
**
** Functions lsm_csr_seek(), lsm_csr_first() and lsm_csr_last() are "seek"
** functions. Whether or not lsm_csr_next and lsm_csr_prev may be called
** successfully also depends on the most recent seek function called on
** the cursor. Specifically:
**
**   * At least one seek function must have been called on the cursor.
**
**   * To call lsm_csr_next(), the most recent call to a seek function must
**     have been either lsm_csr_first() or a call to lsm_csr_seek() specifying
**     LSM_SEEK_GE. 
**
**   * To call lsm_csr_prev(), the most recent call to a seek function must
**     have been either lsm_csr_first() or a call to lsm_csr_seek() specifying
**     LSM_SEEK_GE. 
**
** Otherwise, if the above conditions are not met when lsm_csr_next or 
** lsm_csr_prev is called, LSM_MISUSE is returned and the cursor position
** remains unchanged.
*/
int lsm_csr_next(lsm_cursor *pCsr);
int lsm_csr_prev(lsm_cursor *pCsr);

/* 
** Retrieve data from a database cursor.
*/
int lsm_csr_valid(lsm_cursor *pCsr);
int lsm_csr_key(lsm_cursor *pCsr, void **ppKey, int *pnKey);
int lsm_csr_value(lsm_cursor *pCsr, void **ppVal, int *pnVal);

#ifdef __cplusplus
}  /* End of the 'extern "C"' block */
#endif
#endif /* ifndef _LSM_H */
