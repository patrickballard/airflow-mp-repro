"""The Stack Overflow pattern: a Manager() lock + dict shared with Pool.imap_unordered workers."""
import functools
import multiprocessing as mp


def process(item, lock, shared):
    with lock:
        shared[item] = item * item
    return item


def run_with_manager(method, worker=process, items=40, shutdown=False):
    ctx = mp.get_context(method)  # forced explicitly, whatever the platform default is
    manager = ctx.Manager()       # as in the question: not shut down, not used as a context manager
    lock, shared = manager.Lock(), manager.dict()
    with ctx.Pool(4) as pool:
        for _ in pool.imap_unordered(functools.partial(worker, lock=lock, shared=shared), range(items)):
            pass
    n = len(shared)
    if shutdown:
        manager.shutdown()
    print(f"MP_DONE method={method} items={n} manager_pid={manager._process.pid}", flush=True)
    return n
