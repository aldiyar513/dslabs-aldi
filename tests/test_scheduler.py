from dslabs.scheduler import SimScheduler


def test_callbacks_run_in_due_order_and_see_their_own_time():
    s = SimScheduler()
    seen = []
    s.call_later(300, lambda: seen.append(("c", s.now_ms())))
    s.call_later(100, lambda: seen.append(("a", s.now_ms())))
    s.call_later(200, lambda: seen.append(("b", s.now_ms())))
    s.run_until(1000)
    assert seen == [("a", 100), ("b", 200), ("c", 300)]
    assert s.now_ms() == 1000


def test_same_due_time_runs_first_in_first_out():
    s = SimScheduler()
    seen = []
    for name in "abc":
        s.call_later(50, lambda name=name: seen.append(name))
    s.run_until(50)
    assert seen == ["a", "b", "c"]


def test_cancel_prevents_the_callback():
    s = SimScheduler()
    seen = []
    cancel = s.call_later(10, lambda: seen.append("ran"))
    cancel()
    s.run_until(100)
    assert seen == []
    assert s.pending() == []


def test_run_until_only_runs_what_is_due():
    s = SimScheduler()
    seen = []
    s.call_later(10, lambda: seen.append("early"))
    s.call_later(500, lambda: seen.append("late"))
    s.run_until(100)
    assert seen == ["early"]
    assert [when for when, _ in s.pending()] == [500]


def test_callbacks_may_schedule_more_work_within_the_same_run():
    s = SimScheduler()
    seen = []

    def tick(n: int) -> None:
        seen.append((n, s.now_ms()))
        if n < 3:
            s.call_later(10, lambda: tick(n + 1))

    s.call_later(0, lambda: tick(1))
    s.run_until(100)
    assert seen == [(1, 0), (2, 10), (3, 20)]


def test_run_until_idle_reports_whether_work_remains():
    s = SimScheduler()
    s.call_later(10, lambda: None)
    assert s.run_until_idle(max_ms=1000) is True
    assert s.now_ms() == 10

    def heartbeat() -> None:
        s.call_later(100, heartbeat)

    heartbeat()
    assert s.run_until_idle(max_ms=1000) is False
    assert s.now_ms() == 1000


def test_step_runs_exactly_one_live_event_and_peek_looks_ahead():
    s = SimScheduler()
    seen = []
    s.call_later(10, lambda: seen.append("a"))
    cancel = s.call_later(20, lambda: seen.append("b"))
    cancel()
    s.call_later(30, lambda: seen.append("c"))

    first = s.step()
    assert first and first.number == 1 and first.t_ms == 10 and seen == ["a"]
    assert s.peek().due_ms == 30, "cancelled event is skipped"
    assert s.now_ms() == 10, "peek does not move time"

    second = s.step()
    assert second.number == 2 and second.t_ms == 30 and seen == ["a", "c"]

    idle = s.step()
    assert not idle and idle.t_ms == 30 and str(idle) == "nothing pending @ 30 ms"
    assert s.peek() is None
    assert s.steps == 2


def test_step_loop_runs_to_the_end():
    s = SimScheduler()
    seen = []
    for i in range(3):
        s.call_later(i * 10, lambda i=i: seen.append(i))
    while s.step():
        pass
    assert seen == [0, 1, 2]
