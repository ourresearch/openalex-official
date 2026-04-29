"""Tests for persisted page tracking."""

from openalex_cli.checkpoint import CheckpointManager
from openalex_cli.page_tracker import PageTracker


def test_replay_only_pending_work_after_partial_page_completion(tmp_path):
    manager = CheckpointManager(tmp_path)
    manager.create(filter_str="type:article", content_format="none")
    tracker = PageTracker(tmp_path, manager)

    page = tracker.register_page("*", "cursor-1", ["W1", "W2", "W3"])
    tracker.record_work_result(page.seq, "W1", True, 100, 0, None)
    tracker.record_work_result(page.seq, "W2", False, 0, 0, "boom")

    replay = tracker.replay_pending_work()
    assert [(item.page_seq, item.work_id) for item in replay] == [(page.seq, "W3")]


def test_terminal_pages_commit_in_order_even_if_results_arrive_out_of_order(tmp_path):
    manager = CheckpointManager(tmp_path)
    manager.create(filter_str="type:article", content_format="none")
    tracker = PageTracker(tmp_path, manager)

    page1 = tracker.register_page("*", "cursor-1", ["W1"])
    page2 = tracker.register_page("cursor-1", "cursor-2", ["W2"])

    commit = tracker.record_work_result(page2.seq, "W2", True, 20, 0, None)
    assert commit.committed_pages == 0
    assert manager.get().current_cursor == "*"

    commit = tracker.record_work_result(page1.seq, "W1", True, 10, 0, None)
    assert commit.committed_pages == 2
    assert manager.get().current_cursor == "cursor-2"
    assert manager.get().pages_completed == 2
    assert manager.get().completed_work_ids == {"W1", "W2"}
    assert tracker.replay_pending_work() == []


def test_startup_reconcile_drops_stale_committed_manifest(tmp_path):
    manager = CheckpointManager(tmp_path)
    manager.create(filter_str="type:article", content_format="none")
    tracker = PageTracker(tmp_path, manager)

    page = tracker.register_page("*", "cursor-1", ["W1"])
    tracker.record_work_result(page.seq, "W1", True, 10, 0, None)

    stale = tracker.register_page("cursor-1", "cursor-2", ["W2"])
    manifest = tracker.state_store.list_manifests()[0]
    manifest.work_states["W2"].state = "completed"
    manifest.work_states["W2"].file_size = 5
    tracker.state_store.save_manifest(manifest)
    manager.commit_page("cursor-2", [("W2", 5, 0)], [])

    assert all(item.page_seq != stale.seq for item in tracker.replay_pending_work())
    tracker.startup_reconcile()
    assert tracker.replay_pending_work() == []


def test_resume_cursor_uses_highest_uncommitted_cursor_out(tmp_path):
    manager = CheckpointManager(tmp_path)
    manager.create(filter_str="type:article", content_format="none")
    tracker = PageTracker(tmp_path, manager)

    tracker.register_page("*", "cursor-1", ["W1"])
    tracker.register_page("cursor-1", "cursor-2", ["W2"])

    assert tracker.resume_cursor("*") == "cursor-2"
