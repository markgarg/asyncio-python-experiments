import asyncio
from asyncio import Queue


def send_to_printer(data):
    print("BEGUN printing '{}'".format(data))
    yield from asyncio.sleep(2)
    print("FINISHED printing '{}'".format(data))


def update_operations_backend(data):
    print("BEGUN Sending update to backend via API {}".format(data))
    yield from asyncio.sleep(1)
    print("FINISHED Sending update to backend via API {}".format(data))


@asyncio.coroutine
def print_items(queue_for_printer, printer_id):
    while not queue_for_printer.empty():
        print_item = yield from queue_for_printer.get()
        if print_item.type is POISON_PILL:
            break
        yield from send_to_printer(print_item)
        # TODO: Check status and handle printer error
        yield from update_operations_backend(print_item)


def get_print_items():
    return [
        {"name": "a", "printer_id": 1},
        {"name": "a", "printer_id": 2},
        {"name": "a", "printer_id": 3},
        {"name": "a", "printer_id": 4},
        {"name": "a", "printer_id": 5},
        {"name": "a", "printer_id": 6},
        {"name": "a", "printer_id": 7},
    ]


def get_active_printers():
    return ['1', '2', '3', '4', ]


if __name__ == '__main__':
    # 1. Get the list of active printers
    printers = get_active_printers()

    # 2. Make a queue for each active printer
    queue1 = Queue()
    queue2 = Queue()
    queue3 = Queue()
    queue4 = Queue()

    # 3. Divide the workload per printer into printer specific Queue
    grouped_print_items = group_print_items_by_printer(printers)
    queue_printer_tuples = populate_queues(grouped_print_items)

    loop = asyncio.get_event_loop()

    # 4. Create a task to process the print for each printer
    tasks = [print_items(queue_for_printer, printer_id)
             for (queue_for_printer, printer_id) in queue_printer_tuples]

    # 5. Wait until all the tasks are completed
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
