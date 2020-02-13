import os
import time
from psaw import PushshiftAPI
from func_timeout import FunctionTimedOut, func_set_timeout


@func_set_timeout(5)
def extract_posts_test():
    # API
    api = PushshiftAPI()

    gen = api.search_comments(q='i feel sad')

    max_response_cache = 10000
    cache = []

    start = time.time()
    for c in gen:
        print(c.d_)
        cache.append(c)

        # Omit this test to actually return all results (watch out for the time)
        if len(cache) >= max_response_cache:
            break

    end = time.time()
    print(end - start)  # q = "i feel sad" cache = 10000 ~time = 112 sec


try:
    extract_posts_test()
except FunctionTimedOut:
    print("Function has timed out")
