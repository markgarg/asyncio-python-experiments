"""Example for a print manager function."""
import asyncio
from asyncio import Queue

TYPE_POISON_PILL = "Poison Pill"
PRIORITY_DEFAULT = 5
PRIORITY_POISON_PILL = 1


class PrintManager(object):
    """Fetches data, assigns to printer and does some operations."""

    def __init__(self):
        self.printer_queue_mapping = {}

    def send_to_printer(self, data):
        """Simulate sending data to printer."""
        print("BEGUN printing '{}'".format(data))
        yield from asyncio.sleep(2)
        print("FINISHED printing '{}'".format(data))

    def update_print_completion(self, data):
        """Update result of printing to some system."""
        print("BEGUN Sending update to backend via API {}".format(data))
        yield from asyncio.sleep(1)
        print("FINISHED Sending update to backend via API {}".format(data))

    @asyncio.coroutine
    def print_items(self, queue_for_printer, printer_id):
        """Print the items in the queue.

        For each item in the queue_for_printer, print it and send an
        update when done.
        """
        while not queue_for_printer.empty():
            print_item = yield from queue_for_printer.get()
            if hasattr(print_item, "type") and print_item.type is POISON_PILL:
                break
            try:
                yield from self.send_to_printer(print_item)
            except PrinterError as e:
                print("Printer error {}".format(e))
                self.stop_all_printers()
                self.start(restart=True)
            else:
                yield from self.update_print_completion(print_item)

    def get_print_items(self):
        """Dummyd data with printer_id."""
        return [
            {"name": "a", "status": STATUS_NOT_STARTED, "printer_id": 1},
            {"name": "b", "status": STATUS_NOT_STARTED, "printer_id": 2},
            {"name": "c", "status": STATUS_NOT_STARTED, "printer_id": 3},
            {"name": "d", "status": STATUS_NOT_STARTED, "printer_id": 4},
            {"name": "e", "status": STATUS_NOT_STARTED, "printer_id": 1},
            {"name": "f", "status": STATUS_NOT_STARTED, "printer_id": 2},
            {"name": "g", "status": STATUS_NOT_STARTED, "printer_id": 3},
        ]

    def get_active_printers(self):
        """Get list of active printer_ids."""
        return ['1', '2', '3', '4', ]

    def group_print_items_by_printer(self, printers, item_data):
        """Create a dict with each printer_id as key and data."""
        printer_data_list = {
            "1": [
                {"name": "a", "printer_id": 1},
                {"name": "e", "printer_id": 1},
            ],
            "2": [
                {"name": "b", "printer_id": 2},
                {"name": "f", "printer_id": 2},
            ],
            "3": [
                {"name": "c", "printer_id": 3},
                {"name": "g", "printer_id": 3},
            ],
            "4": [
                {"name": "d", "printer_id": 4},
            ],
        }
        return printer_data_list

    def populate_queues(self, grouped_print_items):
        """Create a queue for each printer and populate items."""
        queue_printer_tuples = []
        queues = [Queue() for _ in grouped_print_items]

        i = 0
        for printer_id, print_item_list in grouped_print_items.items():
            [queues[i].put_nowait((PRIORITY_DEFAULT, item))
             for item in print_item_list]
            queue_printer_tuples.append((queues[i], printer_id))
            self.printer_queue_mapping[printer_id] = queues[i]
            i += 1
        print("populate_queues :{}".format(queue_printer_tuples))
        return queue_printer_tuples

    def start(self, restart=False):
        """Base algorithm for execution."""
        # 1. Get the list of active printers
        printers = self.get_active_printers()

        # 2. Divide the workload per printer into printer specific Queue
        item_data = self.get_print_items()
        grouped_print_items = self.group_print_items_by_printer(printers,
                                                                item_data)
        queue_printer_tuples = self.populate_queues(grouped_print_items)

        loop = asyncio.get_event_loop()

        # 4. Create a task to process the print for each printer
        tasks = [self.print_items(queue_for_printer, printer_id)
                 for (queue_for_printer, printer_id) in queue_printer_tuples]

        # # 5. Wait until all the tasks are completed
        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()
        return

    def stop_all_printers(self):
        """Stop all printers from printing the remaining data."""
        [self.stop_printer(printer_id)
         for printer_id in self.printer_queue_mapping.itemkeys()]

    def stop_printer(self, printer_id):
        """Stop the printer with printer_id from printing the remaining."""
        # TODO: Check if the printer is actually printing something. It may
        # be idle or in Error state.
        self.printer_queue_mapping[printer_id].put_nowait(
            PRIORITY_POISON_PILL, {"type": TYPE_POISON_PILL}
        )


if __name__ == '__main__':
    print_manager = PrintManager()
    print_manager.start()
