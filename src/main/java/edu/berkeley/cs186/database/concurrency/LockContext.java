package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    private LockType acquireEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        LockType lockType = LockType.NL;
        LockContext ctx = this;
        do {
            lockType = lockman.getLockType(transaction, ctx.name);
            ctx = ctx.parent;
        } while (ctx != null && lockType == LockType.NL);
        return lockType;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     * <p>
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException          if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     *                                       transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        //异常
        //只读资源被尝试上锁
        if (readonly) {
            throw new UnsupportedOperationException("the context is readonly");
        }
        LockType existLock = acquireEffectiveLockType(transaction);
        //祖先已持有SIX锁，仍旧尝试请求S锁或IS锁，带来冗余
        if ((lockType == LockType.S || lockType == LockType.IS) && hasSIXAncestor(transaction)) {
            throw new InvalidLockException("invalid request");
        }
        //获取的新锁无法将已有的锁作为前置，例如，无法从IS锁获取到X锁
        if (existLock != LockType.NL && !LockType.canBeParentLock(existLock, lockType)) {
            throw new InvalidLockException("invalid lock");
        }
        lockman.acquire(transaction, name, lockType);
        increaseChildLockNum(transaction, parent, 1);
        return;
    }

    /**
     * Release `transaction`'s lock on `name`.
     * <p>
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException           if no lock on `name` is held by `transaction`
     * @throws InvalidLockException          if the lock cannot be released because
     *                                       doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        //异常
        //只读资源
        if (readonly) {
            throw new UnsupportedOperationException("the context is readonly");
        }
        LockType curLock = getExplicitLockType(transaction);
        //要释放的锁不存在
        if (curLock == LockType.NL) {
            throw new NoLockHeldException("no lock");
        }
        // 子结点还有锁没有被释放
        if (numChildLocks.getOrDefault(transaction.getTransNum(), 0) != 0) {
            throw new InvalidLockException("invalid request");
        }
        lockman.release(transaction, name);
        decreaseChildLockNum(transaction, parent, 1);
        return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     * <p>
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     *                                       `newLockType` lock
     * @throws NoLockHeldException           if `transaction` has no lock
     * @throws InvalidLockException          if the requested lock type is not a
     *                                       promotion or promoting would cause the lock manager to enter an invalid
     *                                       state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     *                                       type B is valid if B is substitutable for A and B is not equal to A, or
     *                                       if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     *                                       be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        //异常
        //只读资源
        if (readonly) {
            throw new UnsupportedOperationException("the context is readonly");
        }
        LockType existLock = getExplicitLockType(transaction);
        //要释放的锁不存在
        if (existLock == LockType.NL) {
            throw new NoLockHeldException("no lock");
            // 重复请求
        } else if (existLock == newLockType) {
            throw new DuplicateLockRequestException("duplicate");
        }
        //请求的锁不是原锁的强化版
        if (!LockType.substitutable(newLockType, existLock)) {
            throw new InvalidLockException("invalid lock");
        }
        //如果目标是SIX锁，就先获取持有S锁和IS锁的所有子结点，然后将他们全部释放
        if (newLockType == LockType.SIX) {
            List<ResourceName> names = sisDescendants(transaction);
            names.add(name);
            decreaseChildLockNum(transaction, this, names.size());
            lockman.acquireAndRelease(transaction, name, newLockType, names);
            increaseChildLockNum(transaction, this, 1);
        } else {
            lockman.promote(transaction, name, newLockType);
        }
        return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     * <p>
     * For example, if a transaction has the following locks:
     * <p>
     * IX(database)
     * /         \
     * IX(table1)    S(table2)
     * /      \
     * S(table1 page3)  X(table1 page5)
     * <p>
     * then after table1Context.escalate(transaction) is called, we should have:
     * <p>
     * IX(database)
     * /         \
     * X(table1)     S(table2)
     * <p>
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     * <p>
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException           if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        //异常
        //只读资源
        if (readonly) {
            throw new UnsupportedOperationException("the context is readonly");
        }
        LockType existLock = getExplicitLockType(transaction);
        //要释放的锁不存在
        if (existLock == LockType.NL) {
            throw new NoLockHeldException("no lock");
        }

        List<ResourceName> names = allChildrenNames(transaction);
        names.add(name);
        decreaseChildLockNum(transaction, this, names.size());
        //将某一粒度的锁提升为S锁或X锁，IS->S, IX/SIX->X
        if (existLock == LockType.IS) {
            lockman.acquireAndRelease(transaction, name, LockType.S, names);
            increaseChildLockNum(transaction, this, 1);
        } else if (existLock == LockType.IX || existLock == LockType.SIX) {
            lockman.acquireAndRelease(transaction, name, LockType.X, names);
            increaseChildLockNum(transaction, this, 1);
        }
        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType lockType = LockType.NL;
        LockContext ctx = this;
        do {
            //如果当前上下文中没有锁，就开始考虑来自父节点影响的隐式的锁类型，不过需要注意的是，意向锁并不会隐式传递给子节点
            //例如父节点获取了SIX锁，则相当于子节点都隐式地获取了S锁
            lockType = lockman.getLockType(transaction, ctx.name);
            if (lockType == LockType.SIX) {
                return LockType.S;
            }
            if (lockType.isIntent()) {
                lockType = LockType.NL;
            }
            ctx = ctx.parent;
        } while (ctx != null && lockType == LockType.NL);
        return lockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     *
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockType lockType = LockType.NL;
        LockContext ctx = this;
        do {
            lockType = lockman.getLockType(transaction, ctx.name);
            if (lockType == LockType.SIX) {
                return true;
            }
            ctx = ctx.parent;
        } while (ctx != null && lockType == LockType.NL);
        return false;
    }

    private List<ResourceName> allChildrenNames(TransactionContext transaction) {
        List<ResourceName> names = new ArrayList<>();
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock lock : locks) {
            if (lock.name.isDescendantOf(name)) {
                names.add(lock.name);
            }
        }
        return names;
    }

    private void increaseChildLockNum(TransactionContext transaction, LockContext parent, int delta) {
        if (parent == null) {
            return;
        }
        Integer num = parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0);
        num += delta;
        parent.numChildLocks.put(transaction.getTransNum(), num);
        increaseChildLockNum(transaction, parent.parent, delta);
    }

    private void decreaseChildLockNum(TransactionContext transaction, LockContext parent, int delta) {
        if (parent == null) {
            return;
        }
        Integer num = parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0);
        if (num == 0) {
            return;
        }
        num -= delta;
        parent.numChildLocks.put(transaction.getTransNum(), num);
        decreaseChildLockNum(transaction, parent.parent, delta);
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     *
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> names = new ArrayList<>();
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock lock : locks) {
            if (lock.lockType == LockType.S || lock.lockType == LockType.IS) {
                if (lock.name.isDescendantOf(this.name)) {
                    names.add(lock.name);
                }
            }
        }
        return names;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

