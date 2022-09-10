package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 * <p>
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 * <p>
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 * <p>
 * This does mean that in the case of:
 * queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if (lock.transactionNum == except) {
                    continue;
                }
                if (!LockType.compatible(lock.lockType, lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            // 如果这把锁对应的事务已经在该资源上有了锁，就进行更新
            for (Lock existLock : locks) {
                if (Objects.equals(existLock.transactionNum, lock.transactionNum)) {
                    existLock.lockType = lock.lockType;
                    return;
                }
            }

            // 否则就是新建锁，但由于该事务可能是头回加入，所以要进行额外的处理
            locks.add(lock);
            if (transactionLocks.containsKey(lock.transactionNum)) {
                transactionLocks.get(lock.transactionNum).add(lock);
            } else {
                ArrayList<Lock> tLocks = new ArrayList<>();
                tLocks.add(lock);
                transactionLocks.put(lock.transactionNum, tLocks);
            }
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            locks.remove(lock);
            transactionLocks.get(lock.transactionNum).remove(lock);
            // 如果事务内已经没有锁，就释放空间防止OOM
            if (transactionLocks.get(lock.transactionNum).isEmpty()) {
                transactionLocks.remove(lock.transactionNum);
            }
            processQueue();
            return;
        }


        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
            request.transaction.prepareBlock();
            return;
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();
            // TODO(proj4_part1): implement

            //不断获取下一条请求，直至阻塞队列为空或者需要的锁仍然无法被获取
            while (requests.hasNext()) {
                LockRequest req = requests.next();
                //对于可以实现的请求，将需要的锁授予对应的事务，并解除事务的阻塞状态。
                if (checkCompatible(req.lock.lockType, req.transaction.getTransNum())) {
                    if (LockType.substitutable(req.lock.lockType, getTransactionLockType(req.transaction.getTransNum()))) {
                        grantOrUpdateLock(req.lock);
                        //如果该请求中还包含要释放的锁，则在此处调用对应的释放方法。
                        if (req.releasedLocks != null && !req.releasedLocks.isEmpty()) {
                            for (Lock releasedLock : req.releasedLocks) {
                                ResourceEntry resourceEntry = getResourceEntry(releasedLock.name);
                                resourceEntry.releaseLock(releasedLock);
                            }
                        }
                    }
                    waitingQueue.remove();
                    req.transaction.unblock();
                } else {
                    return;
                }
            }
            return;
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if (lock.transactionNum.equals(transaction)) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }

    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     * <p>
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     * <p>
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     * <p>
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     *                                       by `transaction` and isn't being released
     * @throws NoLockHeldException           if `transaction` doesn't hold a lock on one
     *                                       or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            //如果要获取的锁类型和当前已经持有的相同则抛出异常，避免处理无用请求
            LockType exist = getLockType(transaction, name);
            ResourceEntry resourceEntry = getResourceEntry(name);
            if (exist == lockType) {
                throw new DuplicateLockRequestException("duplicate request");
            }
            //如果要释放的锁根本不存在，则抛出异常，期间可以收集一会儿要释放的锁，存储到集合中
            List<Lock> releaseLocks = new ArrayList<>();
            boolean noLock = false;
            for (ResourceName releaseName : releaseNames) {
                LockType type = getLockType(transaction, releaseName);
                if (type == LockType.NL) {
                    noLock = true;
                } else {
                    releaseLocks.add(new Lock(releaseName, type, transaction.getTransNum()));
                }
            }
            if (noLock) {
                throw new NoLockHeldException("no lock");
            }
            // 异常处理完毕后，检查锁的兼容性，如果不兼容，则代表当前请求需要被阻塞，修改标记变量，
            // 将请求添加到队列头部（高优先级操作），该请求中应当额外包含当锁被请求到时，要释放的锁列表
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            if (!resourceEntry.checkCompatible(lockType, transaction.getTransNum())) {
                shouldBlock = true;
                LockRequest req = new LockRequest(transaction, newLock, releaseLocks);
                resourceEntry.addToQueue(req, true);
            } else {
                //如果不需要阻塞，则直接授予事务对应的锁，并释放指定的锁
                resourceEntry.grantOrUpdateLock(newLock);
                for (Lock releaseLock : releaseLocks) {
                    getResourceEntry(releaseLock.name).releaseLock(releaseLock);
                }
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     * <p>
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     *                                       `transaction`
     */
    //由于该方法的优先级不高，请求插入在队尾，因此除了锁不兼容，当阻塞队列中有内容时也会被阻塞
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            LockType exist = getLockType(transaction, name);
            ResourceEntry resourceEntry = getResourceEntry(name);
            if (exist == lockType) {
                throw new DuplicateLockRequestException("duplicate request");
            }
            //请求插入在队尾
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            if (!resourceEntry.checkCompatible(lockType, transaction.getTransNum())
                    || !resourceEntry.waitingQueue.isEmpty()) {
                shouldBlock = true;
                LockRequest req = new LockRequest(transaction, newLock);
                resourceEntry.addToQueue(req, false);
            } else {
                resourceEntry.grantOrUpdateLock(newLock);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     * <p>
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            LockType exist = getLockType(transaction, name);
            if (exist == LockType.NL) {
                throw new NoLockHeldException("no lock");
            }
            ResourceEntry resourceEntry = getResourceEntry(name);
            resourceEntry.releaseLock(new Lock(name, exist, transaction.getTransNum()));
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     * <p>
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     * <p>
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     *                                       `newLockType` lock on `name`
     * @throws NoLockHeldException           if `transaction` has no lock on `name`
     * @throws InvalidLockException          if the requested lock type is not a
     *                                       promotion. A promotion from lock type A to lock type B is valid if and
     *                                       only if B is substitutable for A, and B is not equal to A.
     */
    //该方法用于更新锁的类型，属于高优先级操作
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            LockType exist = getLockType(transaction, name);
            ResourceEntry resourceEntry = getResourceEntry(name);
            if (exist == newLockType) {
                throw new DuplicateLockRequestException("duplicate request");
            } else if (exist == LockType.NL) {
                throw new NoLockHeldException("no lock");
            } else if (!LockType.substitutable(newLockType, exist)) {
                throw new InvalidLockException("invalid lock type");
            }
            //阻塞时将请求添加到队首
            Lock newLock = new Lock(name, newLockType, transaction.getTransNum());
            if (!resourceEntry.checkCompatible(newLockType, transaction.getTransNum())) {
                shouldBlock = true;
                LockRequest req = new LockRequest(transaction, newLock);
                resourceEntry.addToQueue(req, true);
            } else {
                resourceEntry.grantOrUpdateLock(newLock);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        return resourceEntry.getTransactionLockType(transaction.getTransNum());
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
