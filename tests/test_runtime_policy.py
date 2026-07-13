import asyncio
import unittest
from unittest.mock import patch

from app.control.proxy import ProxyDirectory
from app.control.proxy.models import EgressMode, EgressNode
from app.dataplane.account import selector
from app.dataplane.account.table import make_empty_table
from app.dataplane.shared.enums import ModeId, PoolId, StatusId
from app.products import _account_selection
from app.products.openai.router import _safe_sse


class _Config:
    def __init__(self, values: dict[str, int]):
        self.values = values

    def get_int(self, key: str, default: int = 0) -> int:
        return self.values.get(key, default)


def _config(values: dict[str, int]):
    config = _Config(values)

    def get_config(key=None, default=None):
        if key is None:
            return config
        return config.get_int(key, default)

    return get_config


def _table(*, auto_quotas: list[int], fail_counts: list[int] | None = None):
    table = make_empty_table()
    fail_counts = fail_counts or [0] * len(auto_quotas)
    for idx, (auto_quota, fail_count) in enumerate(zip(auto_quotas, fail_counts)):
        table._append_slot(
            token=f"token-{idx}",
            pool_id=int(PoolId.BASIC),
            status_id=int(StatusId.ACTIVE),
            quota_auto=auto_quota,
            quota_fast=1,
            quota_expert=1,
            quota_heavy=-1,
            quota_grok_4_3=1,
            quota_console=1,
            total_auto=1 if auto_quota >= 0 else 0,
            total_fast=1,
            total_expert=1,
            total_heavy=0,
            total_grok_4_3=1,
            total_console=1,
            window_auto=3600 if auto_quota >= 0 else 0,
            window_fast=3600,
            window_expert=3600,
            window_heavy=0,
            window_grok_4_3=3600,
            window_console=3600,
            reset_auto=0,
            reset_fast=0,
            reset_expert=0,
            reset_heavy=0,
            reset_grok_4_3=0,
            reset_console=0,
            health=1.0,
            last_use_s=0,
            last_fail_s=0,
            fail_count=fail_count,
            tags=[],
        )
    return table


class RandomAccountPolicyTests(unittest.TestCase):
    def setUp(self):
        selector.set_strategy("random")
        clear = getattr(selector, "clear_recent_selections", None)
        if clear is not None:
            clear()

    def tearDown(self):
        clear = getattr(selector, "clear_recent_selections", None)
        if clear is not None:
            clear()

    def test_random_selection_uses_requested_model_mode(self):
        table = _table(auto_quotas=[-1, 1])
        with (
            patch.object(selector, "get_config", _config({})),
            patch.object(selector.random, "choice", side_effect=lambda seq: min(seq)),
        ):
            selected = selector.select(
                table,
                int(PoolId.BASIC),
                int(ModeId.AUTO),
                now_s=100,
            )

        self.assertEqual(selected, 1)

    def test_historical_failures_do_not_permanently_remove_active_accounts(self):
        table = _table(auto_quotas=[1], fail_counts=[5])
        with patch.object(selector, "get_config", _config({})):
            selected = selector.select(
                table,
                int(PoolId.BASIC),
                int(ModeId.AUTO),
                now_s=100,
            )

        self.assertEqual(selected, 0)

    def test_recent_accounts_are_skipped_when_alternatives_exist(self):
        table = _table(auto_quotas=[1, 1, 1])
        with (
            patch.object(
                selector,
                "get_config",
                _config({"account.selection.recent_exclusion_count": 2}),
            ),
            patch.object(selector.random, "choice", side_effect=lambda seq: min(seq)),
        ):
            selected = [
                selector.select(
                    table,
                    int(PoolId.BASIC),
                    int(ModeId.AUTO),
                    now_s=100 + offset,
                )
                for offset in range(3)
            ]

        self.assertEqual(selected, [0, 1, 2])

    def test_recent_exclusion_falls_back_when_it_covers_the_pool(self):
        table = _table(auto_quotas=[1, 1])
        with (
            patch.object(
                selector,
                "get_config",
                _config({"account.selection.recent_exclusion_count": 500}),
            ),
            patch.object(selector.random, "choice", side_effect=lambda seq: min(seq)),
        ):
            selected = [
                selector.select(
                    table,
                    int(PoolId.BASIC),
                    int(ModeId.AUTO),
                    now_s=100 + offset,
                )
                for offset in range(3)
            ]

        self.assertEqual(selected, [0, 1, 0])


class RequestPolicyTests(unittest.TestCase):
    def test_random_retry_limit_comes_from_dedicated_config(self):
        with (
            patch.object(_account_selection, "current_strategy", return_value="random"),
            patch.object(
                _account_selection,
                "get_config",
                _config({"retry.random_max_retries": 20}),
            ),
        ):
            retries = _account_selection.selection_max_retries()

        self.assertEqual(retries, 20)

    def test_proxy_pool_keeps_the_same_account_on_the_same_proxy(self):
        async def scenario():
            directory = ProxyDirectory()
            directory._egress_mode = EgressMode.PROXY_POOL
            directory._nodes = [
                EgressNode(node_id="a", proxy_url="http://proxy-a"),
                EgressNode(node_id="b", proxy_url="http://proxy-b"),
            ]
            return [
                await directory._pick_proxy_url(affinity_key="account-a")
                for _ in range(4)
            ]

        selected = asyncio.run(scenario())
        self.assertEqual(selected, [selected[0]] * 4)

    def test_proxy_pool_spreads_accounts_across_available_proxies(self):
        async def scenario():
            directory = ProxyDirectory()
            directory._egress_mode = EgressMode.PROXY_POOL
            directory._nodes = [
                EgressNode(node_id="a", proxy_url="http://proxy-a"),
                EgressNode(node_id="b", proxy_url="http://proxy-b"),
            ]
            return {
                await directory._pick_proxy_url(affinity_key=f"account-{idx}")
                for idx in range(32)
            }

        self.assertEqual(
            asyncio.run(scenario()),
            {"http://proxy-a", "http://proxy-b"},
        )

    def test_unaffiliated_requests_do_not_remap_an_account(self):
        async def scenario():
            directory = ProxyDirectory()
            directory._egress_mode = EgressMode.PROXY_POOL
            directory._nodes = [
                EgressNode(node_id="a", proxy_url="http://proxy-a"),
                EgressNode(node_id="b", proxy_url="http://proxy-b"),
            ]
            before = await directory._pick_proxy_url(affinity_key="account-a")
            await directory._pick_proxy_url()
            after = await directory._pick_proxy_url(affinity_key="account-a")
            return before, after

        before, after = asyncio.run(scenario())
        self.assertEqual(before, after)

    def test_proxy_pool_rotates_requests_without_account_affinity(self):
        async def scenario():
            directory = ProxyDirectory()
            directory._egress_mode = EgressMode.PROXY_POOL
            directory._nodes = [
                EgressNode(node_id="a", proxy_url="http://proxy-a"),
                EgressNode(node_id="b", proxy_url="http://proxy-b"),
            ]
            return [await directory._pick_proxy_url() for _ in range(4)]

        self.assertEqual(
            asyncio.run(scenario()),
            ["http://proxy-a", "http://proxy-b", "http://proxy-a", "http://proxy-b"],
        )

    def test_sse_error_event_is_not_followed_by_success_sentinel(self):
        async def broken_stream():
            if False:
                yield ""
            raise RuntimeError("upstream disconnected")

        async def collect():
            return [chunk async for chunk in _safe_sse(broken_stream())]

        chunks = asyncio.run(collect())

        self.assertEqual(len(chunks), 1)
        self.assertTrue(chunks[0].startswith("event: error\n"))
        self.assertNotIn("[DONE]", chunks[0])


if __name__ == "__main__":
    unittest.main()
