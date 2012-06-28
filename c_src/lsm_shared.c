/*
** 2012-01-23
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
** Utilities used to help multiple LSM clients to coexist within the
** same process space.
*/
#include "lsmInt.h"

typedef struct Freelist Freelist;
typedef struct AppendList AppendList;
typedef struct FreelistEntry FreelistEntry;

/*
** TODO: Find homes for these miscellaneous notes. 
**
** FREE-LIST DELTA FORMAT
**
**   The free-list delta consists of three integers:
**
**     1. The number of elements to remove from the start of the free-list.
**     2. If non-zero, a refreed block to append to the free-list.
**     3. Same as (2).
**
** SNAPSHOT ID MANIPULATIONS
**
**   When the database is initialized the worker snapshot id is set to the
**   value read from the checkpoint. Or, if there is no valid checkpoint,
**   to a non-zero default value (e.g. 1).
**
**   The client snapshot is then initialized as a copy of the worker. The
**   client snapshot id is a copy of the worker snapshot id (as read from
**   the checkpoint). The worker snapshot id is then incremented.
**
*/

/*
** Global data. All global variables used by code in this file are grouped
** into the following structure instance.
**
** pDatabase:
**   Linked list of all Database objects allocated within this process.
**   This list may not be traversed without holding the global mutex (see
**   functions enterGlobalMutex() and leaveGlobalMutex()).
*/
static struct SharedData {
  Database *pDatabase;            /* Linked list of all Database objects */
} gShared;

/*
** An instance of the following structure stores the current database free
** block list. The free list is a list of blocks that are not currently
** used by the worker snapshot. Assocated with each block in the list is the
** snapshot id of the most recent snapshot that did actually use the block.
*/
struct Freelist {
  FreelistEntry *aEntry;          /* Free list entries */
  int nEntry;                     /* Number of valid slots in aEntry[] */
  int nAlloc;                     /* Allocated size of aEntry[] */
};
struct FreelistEntry {
  int iBlk;                       /* Block number */
  i64 iId;                        /* Largest snapshot id to use this block */
};

struct AppendList {
  Pgno *aPoint;
  int nPoint;
  int nAlloc;
};

/*
** A snapshot of a database. A snapshot contains all the information required
** to read or write a database file on disk. See the description of struct
** Database below for futher details.
**
** pExport/nExport:
**   pExport points to a buffer containing the serialized (checkpoint) 
**   image of the snapshot. The serialized image is nExport bytes in size. 
*/
struct Snapshot {
  Database *pDatabase;            /* Database this snapshot belongs to */
  Level *pLevel;                  /* Pointer to level 0 of snapshot (or NULL) */
  i64 iId;                        /* Snapshot id */

  Pgno iLogPg;                    /* Log file page to start recovery from */
  u32 iSalt1;                     /* Log file salt value 1 */
  u32 iSalt2;                     /* Log file salt value 2 */
  u32 aDelta[LSM_FREELIST_DELTA_SIZE];

  /* Used by client snapshots only */
  int nRef;                       /* Number of references to this structure */
  void *pExport;                  /* Serialized snapshot image */
  int nExport;                    /* Size of pExport in bytes */

  /* Used by client snapshots only */
  Snapshot *pSnapshotNext;

  /* TODO: Below this point should be moved from Snapshot to Database. */

  /* The following are populated and used by worker snapshots only */
  int nBlock;                     /* Number of blocks tracked by this ss */
  Freelist freelist;              /* Database free-list */

  int bRecordDelta;               /* True when recording freelist delta */
};
#define LSM_INITIAL_LOGPG       1
#define LSM_INITIAL_SNAPSHOT_ID 11
#define LSM_INITIAL_SALT1       0x6c736d21
#define LSM_INITIAL_SALT2       0x78743121

/*
** Database structure. There is one such structure for each distinct 
** database accessed by this process. They are stored in the singly linked 
** list starting at global variable gShared.pDatabase. Database objects are 
** reference counted. Once the number of connections to the associated
** database drops to zero, they are removed from the linked list and deleted.
**
** The primary purpose of the Database structure is to manage Snapshots. A
** snapshot contains the information required to read a database - exactly
** where each array is stored, and where new arrays can be written. A 
** database has one worker snapshot and any number of client snapshots.
**
** WORKER SNAPSHOT
**
**   When a connection is first made to a database and the Database object
**   created, the worker snapshot is initialized to the most recently 
**   checkpointed database state (based on the values in the db header).
**   Any time the database file is written to, either to flush the contents
**   of an in-memory tree or to merge existing segments, the worker snapshot
**   is updated to reflect the modifications.
**
**   The worker snapshot is protected by the worker mutex. The worker mutex
**   must be obtained before a connection begins to modify the database
**   file. After the db file is written, the worker snapshot is updated and
**   the worker mutex released.
**
** CLIENT SNAPSHOTS
**
**   Client snapshots are used by database clients (readers). When a 
**   transaction is opened, the client requests a pointer to a read-only 
**   client snapshot. It is relinquished when the transaction ends. Client 
**   snapshots are reference counted objects.
**
**   When a database is first loaded, the client snapshot is a copy of
**   the worker snapshot. Each time the worker snapshot is checkpointed,
**   the client snapshot is updated with the new checkpointed contents.
**
** THE FREE-BLOCK LIST
**
**   Each Database structure maintains a list of free blocks - the "free-list".
**   There is an entry in the free-list for each block in the database file 
**   that is not used in any way by the worker snapshot.
**
**   Associated with each free block in the free-list is a snapshot id.
**   This is the id of the earliest snapshot that does not require the
**   contents of the block. The block may therefore be reused only after:
**
**     (a) a snapshot with an id equal to or greater than the id associated
**         with the block has been checkpointed into the db header, and
**
**     (b) all existing database clients are using a snapshot with an id
**         equal to or greater than the id stored in the free-list entry.
**
** MULTI-THREADING ISSUES
**
**   Each Database structure carries with it two mutexes - the client 
**   mutex and the worker mutex. In a multi-process version of LSM, these 
**   will be replaced by some other robust locking mechanism. 
**
**   TODO - this description.
*/
struct Database {
#if 0
  lsm_env *pEnv;                  /* Environment handle */
#endif
  char *zName;                    /* Canonical path to database file */
  void *pId;                      /* Database id (file inode) */
  int nId;                        /* Size of pId in bytes */

  Tree *pTree;                    /* Current in-memory tree structure */
  DbLog log;                      /* Database log state object */
  int nPgsz;                      /* Nominal database page size */
  int nBlksz;                     /* Database block size */

  Snapshot *pClient;              /* Client (reader) snapshot */

  Snapshot worker;                /* Worker (writer) snapshot */
  AppendList append;              /* List of appendable points */

  lsm_mutex *pWorkerMutex;        /* Protects the worker snapshot */
  lsm_mutex *pClientMutex;        /* Protects pClient */
  int bDirty;                     /* True if worker has been modified */
  int bRecovered;                 /* True if db does not require recovery */

  int bCheckpointer;              /* True if there exists a checkpointer */
  int bWriter;                    /* True if there exists a writer */
  i64 iCheckpointId;              /* Largest snapshot id stored in db file */

  /* Protected by the global mutex (enterGlobalMutex/leaveGlobalMutex): */
  int nDbRef;                     /* Number of associated lsm_db handles */
  Database *pDbNext;              /* Next Database structure in global list */
};

/*
** Macro that evaluates to true if the snapshot passed as the only argument
** is a worker snapshot. 
*/
#define isWorker(pSnap) ((pSnap)==(&(pSnap)->pDatabase->worker))

/*
** Functions to enter and leave the global mutex. This mutex is used
** to protect the global linked-list headed at 
*/
static int enterGlobalMutex(lsm_env *pEnv){
  lsm_mutex *p;
  int rc = lsmMutexStatic(pEnv, LSM_MUTEX_GLOBAL, &p);
  if( rc==LSM_OK ) lsmMutexEnter(pEnv, p);
  return rc;
}
static void leaveGlobalMutex(lsm_env *pEnv){
  lsm_mutex *p;
  lsmMutexStatic(pEnv, LSM_MUTEX_GLOBAL, &p);
  lsmMutexLeave(pEnv, p);
}

#ifdef LSM_DEBUG
static int holdingGlobalMutex(lsm_env *pEnv){
  lsm_mutex *p;
  lsmMutexStatic(pEnv, LSM_MUTEX_GLOBAL, &p);
  return lsmMutexHeld(pEnv, p);
}
static void assertNotInFreelist(Freelist *p, int iBlk){
  int i; 
  for(i=0; i<p->nEntry; i++){
    assert( p->aEntry[i].iBlk!=iBlk );
  }
}
static void assertMustbeWorker(lsm_db *pDb){
  assert( pDb->pWorker );
  assert( lsmMutexHeld(pDb->pEnv, pDb->pDatabase->pWorkerMutex) );
}
static void assertSnapshotListOk(Database *p){
  Snapshot *pIter;
  i64 iPrev = 0;

  for(pIter=p->pClient; pIter; pIter=pIter->pSnapshotNext){
    assert( pIter==p->pClient || pIter->iId<iPrev );
    iPrev = pIter->iId;
  }
}
#else
# define assertNotInFreelist(x,y)
# define assertMustbeWorker(x)
# define assertSnapshotListOk(x)
#endif


Pgno *lsmSharedAppendList(lsm_db *db, int *pnApp){
  Database *p = db->pDatabase;
  assert( db->pWorker );
  *pnApp = p->append.nPoint;
  return p->append.aPoint;
}

int lsmSharedAppendListAdd(lsm_db *db, Pgno iPg){
  AppendList *pList;
  assert( db->pWorker );
  pList = &db->pDatabase->append;

  assert( pList->nAlloc>=pList->nPoint );
  if( pList->nAlloc<=pList->nPoint ){
    int nNew = pList->nAlloc+8;
    Pgno *aNew = (Pgno *)lsmRealloc(db->pEnv, pList->aPoint, sizeof(Pgno)*nNew);
    if( aNew==0 ) return LSM_NOMEM_BKPT;
    pList->aPoint = aNew;
    pList->nAlloc = nNew;
  }

  pList->aPoint[pList->nPoint++] = iPg;
  return LSM_OK;
}

void lsmSharedAppendListRemove(lsm_db *db, int iIdx){
  AppendList *pList;
  int i;
  assert( db->pWorker );
  pList = &db->pDatabase->append;

  assert( pList->nPoint>iIdx );
  for(i=iIdx+1; i<pList->nPoint;i++){
    pList->aPoint[i-1] = pList->aPoint[i];
  }
  pList->nPoint--;
}

/*
** Append an entry to the free-list.
*/
static int flAppendEntry(lsm_env *pEnv, Freelist *p, int iBlk, i64 iId){

  /* Assert that this is not an attempt to insert a duplicate block number */
  assertNotInFreelist(p, iBlk);

  /* Extend the space allocated for the freelist, if required */
  assert( p->nAlloc>=p->nEntry );
  if( p->nAlloc==p->nEntry ){
    int nNew; 
    FreelistEntry *aNew;

    nNew = (p->nAlloc==0 ? 4 : p->nAlloc*2);
    aNew = (FreelistEntry *)lsmRealloc(pEnv, p->aEntry,
                                       sizeof(FreelistEntry)*nNew);
    if( !aNew ) return LSM_NOMEM_BKPT;
    p->nAlloc = nNew;
    p->aEntry = aNew;
  }

  /* Append the new entry to the freelist */
  p->aEntry[p->nEntry].iBlk = iBlk;
  p->aEntry[p->nEntry].iId = iId;
  p->nEntry++;

  return LSM_OK;
}

/*
** Remove the first entry of the free-list.
*/
static void flRemoveEntry0(Freelist *p){
  int nNew = p->nEntry - 1;
  assert( nNew>=0 );
  memmove(&p->aEntry[0], &p->aEntry[1], sizeof(FreelistEntry) * nNew);
  p->nEntry = nNew;
}

/*
** This function frees all resources held by the Database structure passed
** as the only argument.
*/
static void freeDatabase(lsm_env *pEnv, Database *p){
  if( p ){
    /* Free the mutexes */
    lsmMutexDel(pEnv, p->pClientMutex);
    lsmMutexDel(pEnv, p->pWorkerMutex);

    /* Free the log buffer. */
    lsmStringClear(&p->log.buf);

    /* Free the memory allocated for the Database struct itself */
    lsmFree(pEnv, p);
  }
}

/*
** Return a reference to the shared Database handle for the database 
** identified by canonical path zName. If this is the first connection to
** the named database, a new Database object is allocated. Otherwise, a
** pointer to an existing object is returned.
**
** If successful, *ppDatabase is set to point to the shared Database 
** structure and LSM_OK returned. Otherwise, *ppDatabase is set to NULL
** and and LSM error code returned.
**
** Each successful call to this function should be (eventually) matched
** by a call to lsmDbDatabaseRelease().
*/
int lsmDbDatabaseFind(
  lsm_db *pDb,                    /* Database handle */
  const char *zName               /* Path to db file */
){
  lsm_env *pEnv = pDb->pEnv;
  int rc;                         /* Return code */
  Database *p = 0;                /* Pointer returned via *ppDatabase */
  int nId = 0;
  void *pId = 0;

  assert( pDb->pDatabase==0 );
  rc = lsmFsFileid(pDb, &pId, &nId);
  if( rc!=LSM_OK ) return rc;

  rc = enterGlobalMutex(pEnv);
  if( rc==LSM_OK ){

    /* Search the global list for an existing object. TODO: Need something
    ** better than the strcmp() below to figure out if a given Database
    ** object represents the requested file.  */
    for(p=gShared.pDatabase; p; p=p->pDbNext){
      if( nId==p->nId && 0==memcmp(pId, p->pId, nId) ) break;
    }

    /* If no suitable Database object was found, allocate a new one. */
    if( p==0 ){
      int nName = strlen(zName);
      p = (Database *)lsmMallocZeroRc(pEnv, sizeof(Database)+nId+nName+1, &rc);

      /* Initialize the log handle */
      if( rc==LSM_OK ){
        p->log.cksum0 = LSM_CKSUM0_INIT;
        p->log.cksum1 = LSM_CKSUM1_INIT;
        lsmStringInit(&p->log.buf, pEnv);
      }

      /* Allocate the two mutexes */
      if( rc==LSM_OK ) rc = lsmMutexNew(pEnv, &p->pWorkerMutex);
      if( rc==LSM_OK ) rc = lsmMutexNew(pEnv, &p->pClientMutex);

      /* If no error has occurred, fill in other fields and link the new 
      ** Database structure into the global list starting at 
      ** gShared.pDatabase. Otherwise, if an error has occurred, free any
      ** resources allocated and return without linking anything new into
      ** the gShared.pDatabase list.  */
      if( rc==LSM_OK ){
        p->zName = (char *)&p[1];
        memcpy((void *)p->zName, zName, nName+1);
        p->pId = (void *)&p->zName[nName+1];
        memcpy(p->pId, pId, nId);
        p->nId = nId;
        p->worker.pDatabase = p;
        p->pDbNext = gShared.pDatabase;
        gShared.pDatabase = p;

        p->worker.iLogPg = LSM_INITIAL_LOGPG;
        p->worker.iId = LSM_INITIAL_SNAPSHOT_ID;
        p->worker.iSalt1 = LSM_INITIAL_SALT1;
        p->worker.iSalt2 = LSM_INITIAL_SALT2;
        p->nPgsz = pDb->nDfltPgsz;
        p->nBlksz = pDb->nDfltBlksz;
      }else{
        freeDatabase(pEnv, p);
        p = 0;
      }
    }

    if( p ) p->nDbRef++;
    leaveGlobalMutex(pEnv);
  }

  lsmFree(pEnv, pId);
  pDb->pDatabase = p;
  return rc;
}

static void freeClientSnapshot(lsm_env *pEnv, Snapshot *p){
  Level *pLevel;
  
  assert( p->nRef==0 );
  for(pLevel=p->pLevel; pLevel; pLevel=pLevel->pNext){
    lsmFree(pEnv, pLevel->pSplitKey);
  }
  lsmFree(pEnv, p->pExport);
  lsmFree(pEnv, p);
}


/*
** Release a reference to a Database object obtained from lsmDbDatabaseFind().
** There should be exactly one call to this function for each successful
** call to Find().
*/
void lsmDbDatabaseRelease(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  if( p ){
    enterGlobalMutex(pDb->pEnv);
    p->nDbRef--;
    if( p->nDbRef==0 ){
      int rc = LSM_OK;
      Database **pp;

      /* Remove the Database structure from the linked list. */
      for(pp=&gShared.pDatabase; *pp!=p; pp=&((*pp)->pDbNext));
      *pp = p->pDbNext;

      /* Flush the in-memory tree, if required. If there is data to flush,
      ** this will create a new client snapshot in Database.pClient. The
      ** checkpoint (serialization) of this snapshot may be written to disk
      ** by the following block.  */
      if( p->bDirty || 0==lsmTreeIsEmpty(p->pTree) ){
        rc = lsmFlushToDisk(pDb);
      }

      /* Write a checkpoint, also if required */
      if( rc==LSM_OK && p->pClient ){
        rc = lsmCheckpointWrite(pDb);
      }

      /* If the checkpoint was written successfully, delete the log file */
      if( rc==LSM_OK && pDb->pFS ){
        lsmFsCloseAndDeleteLog(pDb->pFS);
      }

      /* Free the in-memory tree object */
      lsmTreeRelease(pDb->pEnv, p->pTree);

      /* Free the contents of the worker snapshot */
      lsmSortedFreeLevel(pDb->pEnv, p->worker.pLevel);
      lsmFree(pDb->pEnv, p->worker.freelist.aEntry);
      lsmFree(pDb->pEnv, p->append.aPoint);
      
      /* Free the client snapshot */
      if( p->pClient ){
        assert( p->pClient->nRef==1 );
        p->pClient->nRef = 0;
        freeClientSnapshot(pDb->pEnv, p->pClient);
      }

      freeDatabase(pDb->pEnv, p);
    }
    leaveGlobalMutex(pDb->pEnv);
  }
}

Level *lsmDbSnapshotLevel(Snapshot *pSnapshot){
  return pSnapshot->pLevel;
}

void lsmDbSnapshotSetLevel(Snapshot *pSnap, Level *pLevel){
  assert( isWorker(pSnap) );
  pSnap->pLevel = pLevel;
}

void lsmDatabaseDirty(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  assert( lsmMutexHeld(pDb->pEnv, p->pWorkerMutex) );
  if( p->bDirty==0 ){
    p->worker.iId++;
    p->bDirty = 1;
  }
}

int lsmDatabaseIsDirty(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  assert( lsmMutexHeld(pDb->pEnv, p->pWorkerMutex) );
  return p->bDirty;
}

/*
** Get/set methods for the snapshot block-count. These should only be
** used with worker snapshots.
*/
void lsmSnapshotSetNBlock(Snapshot *pSnap, int nNew){
  assert( isWorker(pSnap) );
  pSnap->nBlock = nNew;
}
int lsmSnapshotGetNBlock(Snapshot *pSnap){
  assert( isWorker(pSnap) );
  return pSnap->nBlock;
}

void lsmSnapshotSetCkptid(Snapshot *pSnap, i64 iNew){
  assert( isWorker(pSnap) );
  pSnap->iId = iNew;
}

/*
** Return a pointer to the client snapshot object. Each successful call 
** to lsmDbSnapshotClient() must be matched by an lsmDbSnapshotRelease() 
** call.
*/
#if 0
Snapshot *lsmDbSnapshotClient(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  Snapshot *pRet;
  lsmMutexEnter(pDb->pEnv, p->pClientMutex);
  pRet = p->pClient;
  pRet->nRef++;
  lsmMutexLeave(pDb->pEnv, p->pClientMutex);
  return pRet;
}
#endif

/*
** Return a pointer to the worker snapshot. This call grabs the worker 
** mutex. It is released when the pointer to the worker snapshot is passed 
** to lsmDbSnapshotRelease().
*/
Snapshot *lsmDbSnapshotWorker(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  lsmMutexEnter(pDb->pEnv, p->pWorkerMutex);
  return &p->worker;
}

Snapshot *lsmDbSnapshotRecover(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  Snapshot *pRet = 0;
  lsmMutexEnter(pDb->pEnv, p->pWorkerMutex);
  if( p->bRecovered ){
    lsmFsSetPageSize(pDb->pFS, p->nPgsz);
    lsmFsSetBlockSize(pDb->pFS, p->nBlksz);
    lsmMutexLeave(pDb->pEnv, p->pWorkerMutex);
  }else{
    pRet = &p->worker;
  }
  return pRet;
}

/*
** Set (bVal==1) or clear (bVal==0) the "recovery done" flag.
**
** TODO: Should this be combined with BeginRecovery()/FinishRecovery()?
*/
void lsmDbRecoveryComplete(lsm_db *pDb, int bVal){
  Database *p = pDb->pDatabase;

  assert( bVal==1 || bVal==0 );
  assert( lsmMutexHeld(pDb->pEnv, p->pWorkerMutex) );
  assert( p->pTree );

  p->bRecovered = bVal;
  p->iCheckpointId = p->worker.iId;
  lsmFsSetPageSize(pDb->pFS, p->nPgsz);
  lsmFsSetBlockSize(pDb->pFS, p->nBlksz);
}

void lsmDbSetPagesize(lsm_db *pDb, int nPgsz, int nBlksz){
  Database *p = pDb->pDatabase;
  assert( lsmMutexHeld(pDb->pEnv, p->pWorkerMutex) && p->bRecovered==0 );
  p->nPgsz = nPgsz;
  p->nBlksz = nBlksz;
  lsmFsSetPageSize(pDb->pFS, p->nPgsz);
  lsmFsSetBlockSize(pDb->pFS, p->nBlksz);
}

static void snapshotDecrRefcnt(lsm_env *pEnv, Snapshot *pSnap){
  Database *p = pSnap->pDatabase;

  assertSnapshotListOk(p);
  pSnap->nRef--;
  assert( pSnap->nRef>=0 );
  if( pSnap->nRef==0 ){
    Snapshot *pIter = p->pClient;
    assert( pSnap!=pIter );
    while( pIter->pSnapshotNext!=pSnap ) pIter = pIter->pSnapshotNext;
    pIter->pSnapshotNext = pSnap->pSnapshotNext;
    freeClientSnapshot(pEnv, pSnap);
    assertSnapshotListOk(p);
  }
}

/*
** Release a snapshot reference obtained by calling lsmDbSnapshotWorker()
** or lsmDbSnapshotClient().
*/
void lsmDbSnapshotRelease(lsm_env *pEnv, Snapshot *pSnap){
  if( pSnap ){
    Database *p = pSnap->pDatabase;

    /* If this call is to release a pointer to the worker snapshot, relinquish
    ** the worker mutex.  
    **
    ** If pSnap is a client snapshot, decrement the reference count. When the
    ** reference count reaches zero, free the snapshot object. The decrement
    ** and (nRef==0) test are protected by the database client mutex.
    */
    if( isWorker(pSnap) ){
      lsmMutexLeave(pEnv, p->pWorkerMutex);
    }else{
      lsmMutexEnter(pEnv, p->pClientMutex);
      snapshotDecrRefcnt(pEnv, pSnap);
      lsmMutexLeave(pEnv, p->pClientMutex);
    }
  }
}

/*
** Create a new client snapshot based on the current contents of the worker 
** snapshot. The connection must be the worker to call this function.
*/
int lsmDbUpdateClient(lsm_db *pDb, int nHdrLevel){
  Database *p = pDb->pDatabase;   /* Database handle */
  Snapshot *pOld;                 /* Old client snapshot object */
  Snapshot *pNew;                 /* New client snapshot object */
  int nByte;                      /* Memory required for new client snapshot */
  int rc = LSM_OK;                /* Memory required for new client snapshot */
  int nLevel = 0;                 /* Number of levels in worker snapshot */
  int nRight = 0;                 /* Total number of rhs in worker */
  int nKeySpace = 0;              /* Total size of split keys */
  Level *pLevel;                  /* Used to iterate through worker levels */
  Level **ppLink;                 /* Used to link levels together */
  u8 *pAvail;                     /* Used to divide up allocation */

  /* Must be the worker to call this. */
  assertMustbeWorker(pDb);

  /* Allocate space for the client snapshot and all levels. */
  for(pLevel=p->worker.pLevel; pLevel; pLevel=pLevel->pNext){
    nLevel++;
    nRight += pLevel->nRight;
  }
  nByte = sizeof(Snapshot) 
        + nLevel * sizeof(Level)
        + nRight * sizeof(Segment)
        + nKeySpace;
  pNew = (Snapshot *)lsmMallocZero(pDb->pEnv, nByte);
  if( !pNew ) return LSM_NOMEM_BKPT;
  pNew->pDatabase = p;
  pNew->iId = p->worker.iId;
  pNew->iLogPg = p->worker.iLogPg;
  pNew->iSalt1 = p->worker.iSalt1;
  pNew->iSalt2 = p->worker.iSalt2;

  /* Copy the linked-list of Level structures */
  pAvail = (u8 *)&pNew[1];
  ppLink = &pNew->pLevel;
  for(pLevel=p->worker.pLevel; pLevel && rc==LSM_OK; pLevel=pLevel->pNext){
    Level *p;

    p = (Level *)pAvail;
    memcpy(p, pLevel, sizeof(Level));
    pAvail += sizeof(Level);

    if( p->nRight ){
      p->aRhs = (Segment *)pAvail;
      memcpy(p->aRhs, pLevel->aRhs, sizeof(Segment) * p->nRight);
      pAvail += (sizeof(Segment) * p->nRight);
      lsmSortedSplitkey(pDb, p, &rc);
    }

    /* This needs to come after any call to lsmSortedSplitkey(). Splitkey()
    ** uses data within the Merge object to set p->pSplitKey and co.  */
    p->pMerge = 0;

    *ppLink = p;
    ppLink = &p->pNext;
  }

  /* Create the serialized version of the new client snapshot. */
  if( p->bDirty && rc==LSM_OK ){
    assert( nHdrLevel>0 || p->worker.pLevel==0 );
    rc = lsmCheckpointExport(
        pDb, nHdrLevel, pNew->iId, 1, &pNew->pExport, &pNew->nExport
    );
  }

  if( rc==LSM_OK ){
    /* Initialize the new snapshot ref-count to 1 */
    pNew->nRef = 1;

    lsmDbSnapshotRelease(pDb->pEnv, pDb->pClient);

    /* Install the new client snapshot and release the old. */
    lsmMutexEnter(pDb->pEnv, p->pClientMutex);
    assertSnapshotListOk(p);
    pOld = p->pClient;
    pNew->pSnapshotNext = pOld;
    p->pClient = pNew;
    assertSnapshotListOk(p);
    if( pDb->pClient ){
      pDb->pClient = pNew;
      pNew->nRef++;
    }
    lsmMutexLeave(pDb->pEnv, p->pClientMutex);

    lsmDbSnapshotRelease(pDb->pEnv, pOld);
    p->bDirty = 0;

    /* Upgrade the user connection to the new client snapshot */

  }else{
    /* An error has occurred. Delete the allocated object. */
    freeClientSnapshot(pDb->pEnv, pNew);
  }

  return rc;
}

/*
** Allocate a new database file block to write data to, either by extending
** the database file or by recycling a free-list entry. The worker snapshot 
** must be held in order to call this function.
**
** If successful, *piBlk is set to the block number allocated and LSM_OK is
** returned. Otherwise, *piBlk is zeroed and an lsm error code returned.
*/
int lsmBlockAllocate(lsm_db *pDb, int *piBlk){
  Database *p = pDb->pDatabase;
  Snapshot *pWorker;              /* Worker snapshot */
  Freelist *pFree;                /* Database free list */
  int iRet = 0;                   /* Block number of allocated block */
 
  pWorker = pDb->pWorker;
  pFree = &pWorker->freelist;

  if( pFree->nEntry>0 ){
    /* The first block on the free list was freed as part of the work done
    ** to create the snapshot with id iFree. So, we can reuse this block if
    ** snapshot iFree or later has been checkpointed and all currently 
    ** active clients are reading from snapshot iFree or later.
    */
    Snapshot *pIter;
    i64 iFree = pFree->aEntry[0].iId;
    i64 iInUse;

    /* Both Database.iCheckpointId and the Database.pClient list are 
    ** protected by the client mutex. So grab it here before determining
    ** the id of the oldest snapshot still potentially in use.  */
    lsmMutexEnter(pDb->pEnv, p->pClientMutex);
    assertSnapshotListOk(p);
    for(pIter=p->pClient; pIter->pSnapshotNext; pIter=pIter->pSnapshotNext);
    iInUse = LSM_MIN(pIter->iId, p->iCheckpointId);
    lsmMutexLeave(pDb->pEnv, p->pClientMutex);

    if( iFree<=iInUse ){
      iRet = pFree->aEntry[0].iBlk;
      flRemoveEntry0(pFree);
      assert( iRet!=0 );
      if( pWorker->bRecordDelta ){
        pWorker->aDelta[0]++;
      }
    }
  }

  /* If no block was allocated from the free-list, allocate one at the
  ** end of the file. */
  if( iRet==0 ){
    pWorker->nBlock++;
    iRet = pWorker->nBlock;
  }

  *piBlk = iRet;
  return LSM_OK;
}

/*
** Free a database block. The worker snapshot must be held in order to call 
** this function.
**
** If successful, LSM_OK is returned. Otherwise, an lsm error code (e.g. 
** LSM_NOMEM).
*/
int lsmBlockFree(lsm_db *pDb, int iBlk){
  Snapshot *pWorker = pDb->pWorker;
  int rc = LSM_OK;

  assertMustbeWorker(pDb);
  assert( pWorker->bRecordDelta==0 );
  assert( pDb->pDatabase->bDirty );

  rc = flAppendEntry(pDb->pEnv, &pWorker->freelist, iBlk, pWorker->iId);
  return rc;
}

/*
** Refree a database block. The worker snapshot must be held in order to call 
** this function.
**
** Refreeing is required when a block is allocated using lsmBlockAllocate()
** but then not used. This function is used to push the block back onto
** the freelist. Refreeing a block is different from freeing is, as a refreed
** block may be reused immediately. Whereas a freed block can not be reused 
** until (at least) after the next checkpoint.
*/
int lsmBlockRefree(lsm_db *pDb, int iBlk){
  int rc = LSM_OK;                /* Return code */
  Snapshot *pWorker = pDb->pWorker;

  if( iBlk==pWorker->nBlock ){
    pWorker->nBlock--;
  }else if( pWorker->bRecordDelta ){
    assert( pWorker->aDelta[2]==0 );
    pWorker->aDelta[1 + (pWorker->aDelta[1]!=0)] = iBlk;
  }else{
    rc = flAppendEntry(pDb->pEnv, &pWorker->freelist, iBlk, 0);
  }

  return rc;
}

void lsmFreelistDeltaBegin(lsm_db *pDb){
  Snapshot *pWorker = pDb->pWorker;
  assert( pWorker->bRecordDelta==0 );
  memset(pWorker->aDelta, 0, sizeof(pWorker->aDelta));
  pWorker->bRecordDelta = 1;
}

void lsmFreelistDeltaEnd(lsm_db *pDb){
  Snapshot *pWorker = pDb->pWorker;
  pWorker->bRecordDelta = 0;
}

void lsmFreelistDelta(
  lsm_db *pDb,                    /* Database handle */
  u32 *aDeltaOut                  /* OUT: Copy free-list delta here */
){
  Snapshot *pWorker = pDb->pWorker;
  assert( sizeof(pWorker->aDelta)==(sizeof(u32)*LSM_FREELIST_DELTA_SIZE) );
  memcpy(aDeltaOut, pWorker->aDelta, sizeof(pWorker->aDelta));
}

u32 *lsmFreelistDeltaPtr(lsm_db *pDb){
  return pDb->pWorker->aDelta;
}

/*
** Return the current contents of the free-list as a list of integers.
*/
int lsmSnapshotFreelist(lsm_db *pDb, int **paFree, int *pnFree){
  int rc = LSM_OK;                /* Return Code */
  int *aFree = 0;                 /* Integer array to return via *paFree */
  int nFree;                      /* Value to return via *pnFree */
  Freelist *p;                    /* Database free list object */
  Snapshot *pSnap = pDb->pWorker;

  assert( isWorker(pSnap) );

  p = &pSnap->freelist;
  nFree = p->nEntry;
  if( nFree && paFree ){
    aFree = lsmMallocRc(pDb->pEnv, sizeof(int) * nFree, &rc);
    if( aFree ){
      int i;
      for(i=0; i<nFree; i++){
        aFree[i] = p->aEntry[i].iBlk;
      }
    }
  }

  *pnFree = nFree;
  *paFree = aFree;
  return rc;
}


int lsmSnapshotSetFreelist(lsm_db *pDb, int *aElem, int nElem){
  lsm_env *pEnv = pDb->pEnv;
  int rc = LSM_OK;                /* Return code */
  int i;                          /* Iterator variable */
  int nIgnore;                    /* Number of entries to ignore */
  int iRefree1;                   /* A refreed block (or 0) */
  int iRefree2;                   /* A refreed block (or 0) */
  Freelist *pFree;                /* Database free-list */
  Snapshot *pSnap = pDb->pWorker;

  assert( isWorker(pSnap) );

  nIgnore = pSnap->aDelta[0];
  iRefree1 = pSnap->aDelta[1];
  iRefree2 = pSnap->aDelta[2];

  pFree = &pSnap->freelist;
  for(i=nIgnore; rc==LSM_OK && i<nElem; i++){
    rc = flAppendEntry(pEnv, pFree, aElem[i], 0);
  }

  if( rc==LSM_OK && iRefree1!=0 ) rc = flAppendEntry(pEnv, pFree, iRefree1, 0);
  if( rc==LSM_OK && iRefree2!=0 ) rc = flAppendEntry(pEnv, pFree, iRefree2, 0);

  return rc;
}

/*
** If required, store a new database checkpoint.
**
** The worker mutex must not be held when this is called. This is because
** this function may indirectly call fsync(). And the worker mutex should
** not be held that long (in case it is required by a client flushing an
** in-memory tree to disk).
*/
int lsmCheckpointWrite(lsm_db *pDb){
  Snapshot *pSnap;                /* Snapshot to checkpoint */
  Database *p = pDb->pDatabase;
  int rc = LSM_OK;                /* Return Code */

  assert( pDb->pWorker==0 );

  /* Try to obtain the checkpointer lock, then check if the a checkpoint
  ** is actually required. If successful, and one is, set stack variable
  ** pSnap to point to the client snapshot to checkpoint.  
  */
  lsmMutexEnter(pDb->pEnv, p->pClientMutex);
  pSnap = p->pClient;
  if( p->bCheckpointer==0 && pSnap->iId>p->iCheckpointId ){
    p->bCheckpointer = 1;
    pSnap->nRef++;
  }else{
    pSnap = 0;
  }
  lsmMutexLeave(pDb->pEnv, p->pClientMutex);

  /* Attempt to grab the checkpoint mutex. If the attempt fails, this 
  ** function becomes a no-op. Some other thread is already running
  ** a checkpoint (or at least checking if one is required).  */
  if( pSnap ){
    FileSystem *pFS = pDb->pFS;   /* File system object */
    int iPg = 1;                  /* TODO */
    MetaPage *pPg = 0;            /* Page to write to */
    int doSync;                   /* True to sync the db */

    /* If the safety mode is "off", omit calls to xSync(). */
    doSync = (pDb->eSafety!=LSM_SAFETY_OFF);

    /* Sync the db. To make sure all runs referred to by the checkpoint
    ** are safely on disk. If we do not do this and a power failure occurs 
    ** just after the checkpoint is written into the db header, the
    ** database could be corrupted following recovery.  */
    if( doSync ) rc = lsmFsSyncDb(pFS);

    /* Fetch a reference to the meta-page to write the checkpoint to. */
    if( rc==LSM_OK ) rc = lsmFsMetaPageGet(pFS, 1, iPg, &pPg);

    /* Unless an error has occurred, copy the checkpoint blob into the
    ** meta-page, then release the reference to it (which will flush the
    ** checkpoint into the file).  */
    if( rc!=LSM_OK ){
      lsmFsMetaPageRelease(pPg);
    }else{
      u8 *aData;                  /* Page buffer */
      int nData;                  /* Size of buffer aData[] */
      aData = lsmFsMetaPageData(pPg, &nData);
      assert( pSnap->nExport<=nData );
      memcpy(aData, pSnap->pExport, pSnap->nExport);
      rc = lsmFsMetaPageRelease(pPg);
      pPg = 0;
    }

    /* Sync the db file again. To make sure that the checkpoint just 
    ** written is on the disk.  */
    if( rc==LSM_OK && doSync ) rc = lsmFsSyncDb(pFS);

    /* This is where space on disk is reclaimed. Now that the checkpoint 
    ** has been written to the database and synced, part of the database
    ** log (the part containing the data just synced to disk) is no longer
    ** required and so the space that it was taking up on disk can be 
    ** reused.
    **
    ** It is also possible that database file blocks may be made available
    ** for reuse here. A database file block is free if it is not used by
    ** the most recently checkpointed snapshot, or by a snapshot that is 
    ** in use by any existing database client. And "the most recently
    ** checkpointed snapshot" has just changed.
    */
    lsmMutexEnter(pDb->pEnv, p->pClientMutex);
    if( rc==LSM_OK ){
      lsmLogCheckpoint(pDb, &p->log, lsmCheckpointLogOffset(pSnap->pExport));
      p->iCheckpointId = pSnap->iId;
    }
    p->bCheckpointer = 0;
    snapshotDecrRefcnt(pDb->pEnv, pSnap);
    lsmMutexLeave(pDb->pEnv, p->pClientMutex);
  }

  return rc;
}

/*
** This function is called when a connection is about to run log file
** recovery (read the contents of the log file from disk and create a new
** in memory tree from it). This happens when the very first connection
** starts up and connects to the database.
**
** This sets the connections tree-version handle to one suitable to insert
** the read data into.
**
** Once recovery is complete (regardless of whether or not it is successful),
** lsmFinishRecovery() must be called to release resources locked by
** this function.
*/
int lsmBeginRecovery(lsm_db *pDb){
  int rc;                         /* Return code */
  Database *p = pDb->pDatabase;   /* Shared data handle */

  assert( p && p->pTree==0 );
  assert( pDb->pWorker );
  assert( pDb->pClient==0 );
  assert( pDb->pTV==0 );
  assert( lsmMutexHeld(pDb->pEnv, pDb->pDatabase->pWorkerMutex) );

  rc = lsmTreeNew(pDb->pEnv, pDb->xCmp, &p->pTree);
  if( rc==LSM_OK ){
    assert( pDb->pTV==0 );
    rc = lsmTreeWriteVersion(pDb->pEnv, p->pTree, &pDb->pTV);
  }
  return rc;
}

/*
** Called when recovery is finished.
*/
int lsmFinishRecovery(lsm_db *pDb){
  int rc;
  assert( pDb->pWorker );
  assert( pDb->pClient==0 );
  assert( lsmMutexHeld(pDb->pEnv, pDb->pDatabase->pWorkerMutex) );
  rc = lsmTreeReleaseWriteVersion(pDb->pEnv, pDb->pTV, 1, 0);
  pDb->pTV = 0;
  return rc;
}

/*
** Begin a read transaction. This function is a no-op if the connection
** passed as the only argument already has an open read transaction.
*/
int lsmBeginReadTrans(lsm_db *pDb){
  int rc = LSM_OK;                /* Return code */

  /* No reason a worker connection should be opening a read-transaction. */
  assert( pDb->pWorker==0 );

  if( pDb->pClient==0 ){
    Database *p = pDb->pDatabase;
    lsmMutexEnter(pDb->pEnv, p->pClientMutex);

    assert( pDb->pCsr==0 && pDb->nTransOpen==0 );

    /* If there is no in-memory tree structure, allocate one now */
    if( p->pTree==0 ){
      rc = lsmTreeNew(pDb->pEnv, pDb->xCmp, &p->pTree);
    }

    if( rc==LSM_OK ){
      /* Set the connections client database file snapshot */
      p->pClient->nRef++;
      pDb->pClient = p->pClient;

      /* Set the connections tree-version handle */
      assert( pDb->pTV==0 );
      pDb->pTV = lsmTreeReadVersion(p->pTree);
      assert( pDb->pTV!=0 );
    }

    lsmMutexLeave(pDb->pEnv, p->pClientMutex);
  }

  return rc;
}

/*
** Close the currently open read transaction.
*/
void lsmFinishReadTrans(lsm_db *pDb){
  Snapshot *pClient = pDb->pClient;

  /* Worker connections should not be closing read transactions. And
  ** read transactions should only be closed after all cursors and write
  ** transactions have been closed.  */
  assert( pDb->pWorker==0 );
  assert( pDb->pCsr==0 && pDb->nTransOpen==0 );

  if( pClient ){
    Database *p = pDb->pDatabase;

    lsmDbSnapshotRelease(pDb->pEnv, pDb->pClient);
    pDb->pClient = 0;

    /* Release the in-memory tree version */
    lsmMutexEnter(pDb->pEnv, p->pClientMutex);
    lsmTreeReleaseReadVersion(pDb->pEnv, pDb->pTV);
    pDb->pTV = 0;
    lsmMutexLeave(pDb->pEnv, p->pClientMutex);
  }
}

/*
** Open a write transaction.
*/
int lsmBeginWriteTrans(lsm_db *pDb){
  int rc = LSM_OK;                /* Return code */
  Database *p = pDb->pDatabase;   /* Shared database object */

  lsmMutexEnter(pDb->pEnv, p->pClientMutex);
  assert( p->pTree );
  assert( (pDb->pTV==0)==(pDb->pClient==0) );

  /* There are two reasons the attempt to open a write transaction may fail:
  **
  **   1. There is already a writer.
  **   2. Connection pDb already has an open read transaction, and the read
  **      snapshot is not the most recent version of the database.
  **
  ** If condition 1 is true, then the Database.bWriter flag is set. If the
  ** second is true, then the call to lsmTreeWriteVersion() returns NULL.
  */
  if( p->bWriter ){
    rc = LSM_BUSY;
  }else{
    rc = lsmTreeWriteVersion(pDb->pEnv, p->pTree, &pDb->pTV);
  }

  if( rc==LSM_OK ){
    rc = lsmLogBegin(pDb, &p->log);

    if( rc!=LSM_OK ){
      /* If the call to lsmLogBegin() failed, relinquish the read/write
      ** TreeVersion handle obtained above. The attempt to open a transaction
      ** has failed.  */
      TreeVersion *pWrite = pDb->pTV;
      TreeVersion **ppRestore = (pDb->pClient ? &pDb->pTV : 0);
      pDb->pTV = 0;
      lsmTreeReleaseWriteVersion(pDb->pEnv, pWrite, 0, ppRestore);
    }else if( pDb->pClient==0 ){
      /* Otherwise, if the lsmLogBegin() attempt was successful and the 
      ** client did not have a read transaction open when this function
      ** was called, lsm_db.pClient will still be NULL. In this case, grab 
      ** a reference to the lastest checkpointed snapshot now.  */
      p->pClient->nRef++;
      pDb->pClient = p->pClient;
    }
  }

  if( rc==LSM_OK ){
    p->bWriter = 1;
  }
  lsmMutexLeave(pDb->pEnv, p->pClientMutex);
  return rc;
}

/*
** End the current write transaction. The connection is left with an open
** read transaction. It is an error to call this if there is no open write 
** transaction.
**
** If the transaction was committed, then a commit record has already been
** written into the log file when this function is called. Or, if the
** transaction was rolled back, both the log file and in-memory tree 
** structure have already been restored. In either case, this function 
** merely releases locks and other resources held by the write-transaction.
**
** LSM_OK is returned if successful, or an LSM error code otherwise.
*/
int lsmFinishWriteTrans(lsm_db *pDb, int bCommit){
  Database *p = pDb->pDatabase;
  lsmMutexEnter(pDb->pEnv, p->pClientMutex);

  assert( pDb->pTV && lsmTreeIsWriteVersion(pDb->pTV) );
  assert( p->bWriter );
  p->bWriter = 0;
  lsmTreeReleaseWriteVersion(pDb->pEnv, pDb->pTV, bCommit, &pDb->pTV);

  lsmLogEnd(pDb, &p->log, bCommit);
  lsmMutexLeave(pDb->pEnv, p->pClientMutex);
  return LSM_OK;
}


/*
** This function is called at the beginning of a flush operation (i.e. when
** flushing the contents of the in-memory tree to a segment on disk).
**
** The caller must already be the worker connection.
**
** Also, the caller must have an open write transaction or be in the process
** of shutting down the (shared) database connection. This means we don't
** have to worry about any other connection modifying the in-memory tree
** structure while it is being flushed (although some other clients may be
** reading from it).
*/
int lsmBeginFlush(lsm_db *pDb){

  assert( pDb->pWorker );
  assert( (pDb->pDatabase->bWriter && lsmTreeIsWriteVersion(pDb->pTV))
       || (pDb->pTV==0 && holdingGlobalMutex(pDb->pEnv))
  );

  if( pDb->pTV==0 ){
    pDb->pTV = lsmTreeRecoverVersion(pDb->pDatabase->pTree);
  }
  return LSM_OK;
}

/*
** This is called to indicate that a "flush-tree" operation has finished.
** If the second argument is true, a new in-memory tree is allocated to
** hold subsequent writes.
*/
int lsmFinishFlush(lsm_db *pDb, int bEmpty){
  Database *p = pDb->pDatabase;
  int rc = LSM_OK;

  assert( pDb->pWorker );
  assert( pDb->pTV && (p->nDbRef==0 || lsmTreeIsWriteVersion(pDb->pTV)) );
  lsmMutexEnter(pDb->pEnv, p->pClientMutex);

  if( bEmpty ){
    if( p->bWriter ){
      lsmTreeReleaseWriteVersion(pDb->pEnv, pDb->pTV, 1, 0);
    }
    pDb->pTV = 0;
    lsmTreeRelease(pDb->pEnv, p->pTree);

    if( p->nDbRef>0 ){
      rc = lsmTreeNew(pDb->pEnv, pDb->xCmp, &p->pTree);
    }else{
      /* This is the case if the Database object is being deleted */
      p->pTree = 0;
    }
  }

  if( p->bWriter ){
    assert( pDb->pClient );
    if( 0==pDb->pTV ) rc = lsmTreeWriteVersion(pDb->pEnv, p->pTree, &pDb->pTV);
  }else{
    pDb->pTV = 0;
  }
  lsmMutexLeave(pDb->pEnv, p->pClientMutex);
  return rc;
}

/*
** Return a pointer to the DbLog object associated with connection pDb.
** Allocate and initialize it if necessary.
*/
DbLog *lsmDatabaseLog(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  return &p->log;
}

/*
** Return non-zero if the caller is holding the client mutex.
*/
#ifdef LSM_DEBUG
int lsmHoldingClientMutex(lsm_db *pDb){
  return lsmMutexHeld(pDb->pEnv, pDb->pDatabase->pClientMutex);
}
#endif
