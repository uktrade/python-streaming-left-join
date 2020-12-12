
class UnusedDataException(ValueError):
    pass

def join(left_iter_left_key, *right_iters_with_keys):
    unused = False
    left_iter, left_key = left_iter_left_key

    # Convert iterables to iterators, since we'll be iterating with raw `next` calls
    right_iters_with_keys = tuple((iter(right_iter), right_key) for right_iter, right_key in right_iters_with_keys)

    # Magic "left" item to match all right items to empty the right iterables at the end
    match_any = object()

    def right_gen(right_iter, right_key):
        right_item_needed = object()
        right_item = right_item_needed

        def right_gen_for(left_item):
            nonlocal right_item

            while True:
                if right_item is right_item_needed:
                    try:
                        right_item = next(right_iter)
                    except StopIteration:
                        # The right iterable ended before the left
                        break

                if left_item is match_any or left_key(left_item) == right_key(right_item):
                    yield right_item
                    right_item = right_item_needed
                else:
                    break

        return right_gen_for

    right_gens = [
        right_gen(right_iter, right_key)
        for right_iter, right_key in right_iters_with_keys
    ]

    for left_item in left_iter:
        yield (left_item,) + tuple((list(right_gen_for(left_item))) for right_gen_for in right_gens)

    for right_gen_for in right_gens:
        for item in right_gen_for(match_any):
            unused = True

    if unused:
        raise UnusedDataException()
