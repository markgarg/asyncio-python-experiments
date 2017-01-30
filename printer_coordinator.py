"""Example for a print manager function."""
import asyncio
from asyncio import PriorityQueue

TYPE_POISON_PILL = "Poison Pill"
PRIORITY_DEFAULT = 5
PRIORITY_POISON_PILL = 1
STATUS_NOT_STARTED = 'Not Started'


class PrintItem(object):
    """Representation of an item to be printed."""

    def __init__(self, name, printer_id, status=STATUS_NOT_STARTED):
        self.name = name
        self.printer_id = printer_id
        self.status = status

    def __eq__(self, other):
        """Dummy implementation."""
        return isinstance(other, self.__class__)\
            and self.name == other.name and self.status == other.status

    def __hash__(self, other):
        """Dummy implementation."""
        hash((self.name, self.status))

    def __lt__(self, other):
        """Dummy implementation."""
        return self.__eq__(other) < 0

    def __gt__(self, other):
        """Dummy implementation."""
        return self.__eq__(other) > 0

    def __le__(self, other):
        """Dummy implementation."""
        return self.__eq__(other) <= 0

    def __ge__(self, other):
        """Dummy implementation."""
        return self.__eq__(other) >= 0

    def __ne__(self, other):
        """Dummy implementation."""
        return self.__eq__(other) != 0

    def __str__(self):
        return "PrintItem '{}'".format(self.name)

    def __repr__(self):
        return "PrintItem '{}'".format(self.name)


class PrintManager(object):
    """Fetches data, assigns to printer and does some operations."""

    def __init__(self):
        self.printer_queue_mapping = {}

    @asyncio.coroutine
    def send_to_printer(self, data):
        """Simulate sending data to printer."""
        print("BEGUN printing {}".format(data[1]))
        yield from asyncio.sleep(2)
        # TODO: Update database
        print("FINISHED printing {}".format(data[1]))

    @asyncio.coroutine
    def update_print_completion(self, data):
        """Update result of printing to some system."""
        print("BEGUN Sending update to backend via API {}".format(data[1]))
        yield from asyncio.sleep(1)
        print("FINISHED Sending update to backend via API {}".format(data[1]))

    @asyncio.coroutine
    def print_items(self, queue_for_printer, printer_id):
        """Print the items in the queue.

        For each item in the queue_for_printer, print it and send an
        update when done.
        """
        while not queue_for_printer.empty():
            print_item = queue_for_printer.get_nowait()
            if hasattr(print_item, "type") and print_item.type is POISON_PILL:
                break
            try:
                yield from self.send_to_printer(print_item)
            except PrinterError as e:
                print("Printer error {}".format(e))
                self.handle_printer_error(printer_id)
                break
            else:
                yield from self.update_print_completion(print_item)

    def get_print_items(self):
        """Dummyd data with printer_id."""
        return [
            PrintItem("a", 1, STATUS_NOT_STARTED),
            PrintItem("b", 2, STATUS_NOT_STARTED),
            PrintItem("c", 3, STATUS_NOT_STARTED),
            PrintItem("d", 4, STATUS_NOT_STARTED),
            PrintItem("e", 1, STATUS_NOT_STARTED),
            PrintItem("f", 2, STATUS_NOT_STARTED),
            PrintItem("g", 3, STATUS_NOT_STARTED),
        ]

    def get_active_printers(self):
        """Get list of active printer_ids."""
        return ['1', '2', '3', '4', ]

    def group_print_items_by_printer(self, printers, item_data):
        """Create a dict with each printer_id as key and data."""
        printer_data_list = {
            "1": [
                PrintItem("a", 1, STATUS_NOT_STARTED),
                PrintItem("e", 1, STATUS_NOT_STARTED),
            ],
            "2": [
                PrintItem("b", 2, STATUS_NOT_STARTED),
                PrintItem("f", 2, STATUS_NOT_STARTED),
            ],
            "3": [
                PrintItem("c", 3, STATUS_NOT_STARTED),
                PrintItem("g", 3, STATUS_NOT_STARTED),
            ],
            "4": [
                PrintItem("d", 4, STATUS_NOT_STARTED),
            ],
        }
        return printer_data_list

    def populate_queues(self, grouped_print_items):
        """Create a queue for each printer and populate items."""
        queue_printer_tuples = []
        queues = [PriorityQueue() for _ in grouped_print_items]

        i = 0
        for printer_id, print_item_list in grouped_print_items.items():
            [queues[i].put_nowait((PRIORITY_DEFAULT, item))
             for item in print_item_list]
            queue_printer_tuples.append((queues[i], printer_id))
            self.printer_queue_mapping[printer_id] = queues[i]
            i += 1
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

        # 3. Create a task to process the print for each printer
        tasks = [self.print_items(queue_for_printer, printer_id)
                 for (queue_for_printer, printer_id) in queue_printer_tuples]

        # 4. Wait until all the tasks are completed
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

    def handle_printer_error(self, printer_id):
        """Handle printer error by moving items to other printer's queue.

        Also make an API call to let the caller know that printer has got
        an issue.
        """
        # Move remaining items in the printer queue to other printers
        printer_queue = self.printer_queue_mapping[printer_id]
        random_printer = '2'
        other_printer_queue = self.printer_queue_mapping[random_printer]
        while not printer_queue.empty():
            other_printer_queue.put_nowait(printer_queue.get_nowait())

        # TODO:API call to backend to inform status
        return None


if __name__ == '__main__':
    print_manager = PrintManager()
    print_manager.start()
