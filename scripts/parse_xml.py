"""Fix 3: the question's spawn code as a standalone parser script, run by subprocess.run from the task.

Standalone, its __main__ is this file (guarded below), not the airflow CLI, so spawned workers start cleanly.
"""
import functools
import multiprocessing as mp
import sys
import xml.etree.ElementTree as ET


def parse(doc, lock, results):
    root = ET.fromstring(doc)
    with lock:
        results[root.get("id")] = len(root.findall("item"))
    return root.get("id")


def main(n):
    docs = [f'<doc id="{i}">' + "<item/>" * (i % 7 + 1) + "</doc>" for i in range(n)]
    ctx = mp.get_context("spawn")
    with ctx.Manager() as manager:
        lock, results = manager.Lock(), manager.dict()
        with ctx.Pool(4) as pool:
            for _ in pool.imap_unordered(functools.partial(parse, lock=lock, results=results), docs):
                pass
        print(f"MP_DONE method=spawn(script) items={len(results)}", flush=True)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 40)
