"""Fix (a): the same fork + Manager + Pool code, run as its own script by subprocess.run."""
import functools
import multiprocessing as mp


def process(item, lock, shared):
    with lock:
        shared[item] = item * item
    return item


if __name__ == "__main__":
    ctx = mp.get_context("fork")
    manager = ctx.Manager()
    lock, shared = manager.Lock(), manager.dict()
    with ctx.Pool(4) as pool:
        for _ in pool.imap_unordered(functools.partial(process, lock=lock, shared=shared), range(40)):
            pass
    print(f"MP_DONE method=fork(script) items={len(shared)}", flush=True)
