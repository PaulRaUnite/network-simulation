from typing import Any, Iterable, Dict, Optional, Callable, NamedTuple

from sortedcontainers import SortedDict

NewEvent = NamedTuple("NewEvent", (("event", Any), ("time_shift", int)))


class Timeline:
    def __init__(self):
        self._time_cells = SortedDict()

    def add(self, event: Any, occurring: int):
        if occurring in self._time_cells:
            self._time_cells[occurring].append(event)
        else:
            self._time_cells[occurring] = [event]

    def __iter__(self):
        while self._time_cells:
            occurring, event_list = self._time_cells.popitem(0)
            for event in event_list:
                yield occurring, event


class EventLoop:
    def __init__(
            self,
            initial_events: Iterable[NewEvent],
            handlers: Dict[type, Callable[[Any], Iterable[NewEvent]]],
            end_time: Optional[int] = None,
    ):
        self.timeline = Timeline()
        self.handlers = handlers
        self.end_time = end_time

        for event, occurring in initial_events:
            self.timeline.add(event, occurring)

    def handle_event(self, occurring: int, event: Any):
        new_events = self.handlers[type(event)](event)
        for event, time_shift in new_events:
            self.timeline.add(event, occurring + time_shift)

    def run(self):
        for occurring, event in self.timeline:
            if self.end_time and occurring > self.end_time:
                break
            self.handle_event(occurring, event)
