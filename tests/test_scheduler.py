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
