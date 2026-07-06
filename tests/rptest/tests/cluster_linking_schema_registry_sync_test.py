# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# End-to-end coverage for the shadow-link Schema Registry "API mode" sync: a
# reference-heavy set of source schemas is replicated into the destination
# cluster's Schema Registry over the HTTP source reader, including schemas added
# while a sync is in flight and schemas added after a sync (picked up by the
# next full sync). The layered diamond DAG mirrors the reconciler
# `concurrent_stress` unit test at a larger scale, so the concurrent fetch/import
# path is exercised against a real registry. Plaintext, default context, no auth
# (auth/TLS coverage is tracked separately).

import json
from typing import Any

import google.protobuf.duration_pb2
import google.protobuf.field_mask_pb2
from ducktape.utils.util import wait_until

from rptest.clients.admin.proto.redpanda.core.admin.v2 import shadow_link_pb2
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import TestContext, cluster
from rptest.services.multi_cluster_services import SecondaryClusterArgs
from rptest.services.redpanda import SchemaRegistryConfig
from rptest.tests.cluster_linking_test_base import ShadowLinkTestBase
from rptest.tests.schema_registry_test import SchemaRegistryRedpandaClient

NS = "com.acme"
LINK_NAME = "sr-sync"

Pair = tuple[str, int]


class SchemaRegistrySyncE2ETest(ShadowLinkTestBase):
    """Source cluster SR -> destination cluster SR over the shadow-link HTTP
    source reader, at scale and with concurrent / post-sync additions."""

    # A layered diamond DAG (top -> mid -> leaf), like the reconciler
    # concurrent_stress unit test but larger: adjacent referrers share referents,
    # so the reconciler must import referents-first while several fibers fetch
    # over the (serialized) HTTP source connection.
    LEAVES = 30
    MIDS = 15
    TOPS = 8
    # Added to the source right after the link is created, racing the first
    # in-flight sync.
    EXTRA_LEAVES = 10
    EXTRA_MIDS = 10

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(
            test_context,
            # Source (secondary) cluster runs a Schema Registry too.
            secondary_cluster_args=SecondaryClusterArgs(
                schema_registry_config=SchemaRegistryConfig()
            ),
            # Destination (primary) cluster Schema Registry.
            schema_registry_config=SchemaRegistryConfig(),
            *args,
            **kwargs,
        )

    # --- schema builders ---------------------------------------------------

    def _record(self, name: str, fields: list[dict]) -> dict:
        return {"type": "record", "name": name, "namespace": NS, "fields": fields}

    def _leaf_schema(self, i: int, evolve: bool = False) -> dict:
        fields: list[dict] = [{"name": "v", "type": "string"}]
        if evolve:
            # Backward-compatible new version: an added field with a default.
            fields.append({"name": "extra", "type": "long", "default": 0})
        return self._record(f"Leaf{i}", fields)

    def _mid_schema(self, i: int, la: int, lb: int) -> dict:
        return self._record(
            f"Mid{i}",
            [
                {"name": "a", "type": f"{NS}.Leaf{la}"},
                {"name": "b", "type": f"{NS}.Leaf{lb}"},
            ],
        )

    def _top_schema(self, i: int, ma: int, mb: int) -> dict:
        return self._record(
            f"Top{i}",
            [
                {"name": "x", "type": f"{NS}.Mid{ma}"},
                {"name": "y", "type": f"{NS}.Mid{mb}"},
            ],
        )

    def _large_schema(self, i: int, default_bytes: int) -> dict:
        # Inflate the raw stored definition with a big field default. Defaults
        # are kept verbatim in the stored body the reconciler budgets by
        # (unlike doc, which canonicalization may drop), so the memory budget
        # sees a genuinely large body.
        return self._record(
            f"Big{i}",
            [{"name": "v", "type": "string", "default": "x" * default_bytes}],
        )

    # --- registration helpers ----------------------------------------------

    def _register(
        self,
        client: SchemaRegistryRedpandaClient,
        subject: str,
        schema: dict,
        references: list[dict] | None = None,
    ) -> int:
        payload: dict[str, Any] = {"schema": json.dumps(schema)}
        if references:
            payload["references"] = references
        resp = client.post_subjects_subject_versions(
            subject=subject, data=json.dumps(payload)
        )
        assert resp.status_code == 200, f"register {subject} failed: {resp.text}"
        return resp.json()["id"]

    def _ref(self, name: str, subject: str, version: int = 1) -> dict:
        return {"name": name, "subject": subject, "version": version}

    def _add_leaf(
        self,
        src: SchemaRegistryRedpandaClient,
        i: int,
        evolve: bool = False,
        version: int = 1,
    ) -> Pair:
        # `version` is the subject version this registration produces (1 for a
        # fresh subject, 2 for the evolved leaf-0); it is not the schema id that
        # _register returns.
        self._register(src, f"leaf-{i}-value", self._leaf_schema(i, evolve))
        return (f"leaf-{i}-value", version)

    def _add_mid(
        self, src: SchemaRegistryRedpandaClient, i: int, la: int, lb: int
    ) -> Pair:
        self._register(
            src,
            f"mid-{i}-value",
            self._mid_schema(i, la, lb),
            references=[
                self._ref(f"{NS}.Leaf{la}", f"leaf-{la}-value"),
                self._ref(f"{NS}.Leaf{lb}", f"leaf-{lb}-value"),
            ],
        )
        return (f"mid-{i}-value", 1)

    def _add_top(
        self, src: SchemaRegistryRedpandaClient, i: int, ma: int, mb: int
    ) -> Pair:
        self._register(
            src,
            f"top-{i}-value",
            self._top_schema(i, ma, mb),
            references=[
                self._ref(f"{NS}.Mid{ma}", f"mid-{ma}-value"),
                self._ref(f"{NS}.Mid{mb}", f"mid-{mb}-value"),
            ],
        )
        return (f"top-{i}-value", 1)

    def _seed_initial_dag(self, src: SchemaRegistryRedpandaClient) -> list[Pair]:
        # Register referents before referrers (the source SR requires it); the
        # reconciler rediscovers the order itself on the destination.
        pairs: list[Pair] = []
        for i in range(self.LEAVES):
            pairs.append(self._add_leaf(src, i))
        for i in range(self.MIDS):
            pairs.append(
                self._add_mid(src, i, (2 * i) % self.LEAVES, (2 * i + 1) % self.LEAVES)
            )
        for i in range(self.TOPS):
            pairs.append(
                self._add_top(src, i, (2 * i) % self.MIDS, (2 * i + 1) % self.MIDS)
            )
        return pairs

    # --- link creation -----------------------------------------------------

    def _create_sr_link(
        self,
        source_url: str | None = None,
        full_sync_interval_sec: int = 2,
        source_filter_subjects: list[str] | None = None,
        source_filter_contexts: list[str] | None = None,
        exact_context_map: dict[str, str] | None = None,
    ) -> str:
        # Create a shadow link that syncs only the Schema Registry, in API mode,
        # pointing at the source cluster's SR endpoint (or an explicit URL, e.g.
        # a bad one to exercise the unavailable path). Short intervals so
        # subsequent full syncs land quickly. An optional exact-subject filter
        # scopes replication. Returns the source URL used.
        if source_url is None:
            source_url = self.source_cluster_service.schema_reg(limit=1)
        req = self.create_default_link_request(
            link_name=LINK_NAME,
            mirror_all_topics=False,
            mirror_all_groups=False,
            mirror_all_acls=False,
        )
        api = shadow_link_pb2.SchemaRegistrySyncOptions.ShadowSchemaRegistryApi(
            source_url=source_url,
            tail_interval=google.protobuf.duration_pb2.Duration(seconds=2),
            full_sync_interval=google.protobuf.duration_pb2.Duration(
                seconds=full_sync_interval_sec
            ),
        )
        if source_filter_subjects is not None:
            api.source_filter.subjects.extend(source_filter_subjects)
        if source_filter_contexts is not None:
            api.source_filter.contexts.extend(source_filter_contexts)
        if exact_context_map is not None:
            for source, destination in exact_context_map.items():
                api.destination.exact.mappings.add(
                    source=source, destination=destination
                )
        req.shadow_link.configurations.schema_registry_sync_options.shadow_schema_registry_api.CopyFrom(
            api
        )
        self.create_link_with_request(req=req)
        return source_url

    # --- verification ------------------------------------------------------

    def _schema_view(
        self, client: SchemaRegistryRedpandaClient, subject: str, version: int
    ) -> dict | None:
        # The canonical stored form of one (subject, version): the schema ID,
        # the parsed schema body, and its references. Returns None when absent.
        resp = client.get_subjects_subject_versions_version(subject, version)
        if resp.status_code != 200:
            return None
        body = resp.json()
        return {
            "id": body["id"],
            "schema": json.loads(body["schema"]),
            "references": [
                {"name": r["name"], "subject": r["subject"], "version": r["version"]}
                for r in body.get("references", [])
            ],
        }

    def _wait_synced(
        self,
        src: SchemaRegistryRedpandaClient,
        dest: SchemaRegistryRedpandaClient,
        pairs: list[Pair],
        timeout_sec: int = 120,
    ):
        # Compares the destination against the source's stored form (schema ID,
        # body, and references) so the assertion does not depend on whether the
        # SR canonicalizes on registration. A preserved ID is the point of
        # IMPORT-mode replication.
        def synced() -> bool:
            missing = 0
            for subject, version in pairs:
                want = self._schema_view(src, subject, version)
                assert want is not None, f"source missing {subject} v{version}"
                if self._schema_view(dest, subject, version) != want:
                    missing += 1
            if missing:
                self.logger.debug(f"{missing}/{len(pairs)} not yet synced")
            return missing == 0

        wait_until(
            synced,
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg="schemas did not sync to destination Schema Registry",
        )

    def _sr_versions(
        self, client: SchemaRegistryRedpandaClient, subject: str, deleted: bool
    ) -> set[int]:
        resp = client.get_subjects_subject_versions(subject, deleted=deleted)
        return set(resp.json()) if resp.status_code == 200 else set()

    def _version_state(
        self, client: SchemaRegistryRedpandaClient, subject: str
    ) -> tuple[frozenset[int], frozenset[int]]:
        # (active, soft-deleted) versions of a subject. A soft-deleted version
        # is listed only with deleted=true; a fully soft-deleted subject has an
        # empty active set.
        active = self._sr_versions(client, subject, deleted=False)
        every = self._sr_versions(client, subject, deleted=True)
        return frozenset(active), frozenset(every - active)

    def _wait_delete_synced(
        self,
        src: SchemaRegistryRedpandaClient,
        dest: SchemaRegistryRedpandaClient,
        subjects: list[str],
        timeout_sec: int = 120,
    ):
        # The destination's per-subject (active, deleted) version partition must
        # converge to the source's.
        def synced() -> bool:
            for subject in subjects:
                if self._version_state(dest, subject) != self._version_state(
                    src, subject
                ):
                    return False
            return True

        wait_until(
            synced,
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg="soft-delete state did not converge to the source",
        )

    # --- status-counter checks ---------------------------------------------

    # Counters expected to stay zero in the override-free DAG tests: those DAGs
    # register no mode or compatibility overrides, so mode/config replication is
    # a no-op, and unsupported-feature handling is unimplemented. A test that
    # sets overrides (test_schema_registry_api_sync_compatibility) asserts the
    # compatibility counter advances.
    EXPECTED_ZERO_COUNTERS = (
        "compatibility_configs_changed",
        "modes_changed",
        "unsupported_features_removed",
    )

    def _admin_sr_status(self):
        link = self.get_link(LINK_NAME)
        return link.status.schema_registry_sync_status

    def _observe_current_full_sync(self, timeout_sec: int = 60):
        # While a sync runs, the status exposes current_sync; capture it during
        # the first (longest-running) full sync of the seeded DAG and confirm it
        # is a FULL sync. Tail sync is not implemented yet, so FULL is the only
        # type produced -- this pins the reported sync_type regardless.
        observed: dict[str, Any] = {}

        def in_progress() -> bool:
            sr = self._admin_sr_status()
            if sr.HasField("current_sync"):
                observed["sync_type"] = sr.current_sync.sync_type
                return True
            return False

        wait_until(
            in_progress,
            timeout_sec=timeout_sec,
            backoff_sec=0.2,
            err_msg="never observed a Schema Registry sync in progress",
        )
        assert (
            observed["sync_type"] == shadow_link_pb2.SCHEMA_REGISTRY_SYNC_TYPE_FULL
        ), observed

    def _verify_counters(self, all_pairs: list[Pair]):
        expected_versions = len(all_pairs)
        expected_subjects = len({s for s, _ in all_pairs})

        # The counters reflect the most recently completed full sync; give the
        # task a couple of full-sync cycles to re-scan after the last addition.
        def counters_ready() -> bool:
            sr = self._admin_sr_status()
            inv = sr.inventory
            totals = sr.totals_since_task_start
            return (
                inv.selected_source_subjects == expected_subjects
                and inv.selected_source_subject_versions == expected_versions
                and inv.destination_subjects == expected_subjects
                and inv.destination_subject_versions == expected_versions
                and totals.subject_versions_changed == expected_versions
                and totals.errors == 0
            )

        wait_until(
            counters_ready,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="SR sync status counters did not reach the expected values",
        )

        sr = self._admin_sr_status()
        self._log_counters("admin API", sr)

        # Timestamps: the last completed full sync is present with finish at or
        # after start; the cumulative summary carries the task start time and,
        # being open-ended, has no finish time.
        assert sr.HasField("last_full_sync"), sr
        lfs = sr.last_full_sync
        assert lfs.HasField("start_time") and lfs.HasField("finish_time"), lfs
        assert lfs.finish_time.ToNanoseconds() >= lfs.start_time.ToNanoseconds(), lfs

        # Mapped-but-unimplemented counters must be zero (flagged above).
        totals = sr.totals_since_task_start
        # The cumulative summary carries the task start time (stamped once on the
        # task's first run) and, being open-ended, has no finish time.
        assert totals.HasField("start_time"), totals
        assert totals.start_time.seconds > 0, totals
        assert not totals.HasField("finish_time"), totals
        for name in self.EXPECTED_ZERO_COUNTERS:
            assert getattr(totals, name) == 0, (
                f"unexpected {name}={getattr(totals, name)}"
            )

        # Cross-check the rpk `shadow status` rendering against the admin API.
        self._verify_rpk_status(expected_subjects, expected_versions)

    def _log_counters(self, source: str, sr):
        inv = sr.inventory
        totals = sr.totals_since_task_start
        self.logger.info(
            f"[{source}] inventory: "
            f"selected_source_subjects={inv.selected_source_subjects} "
            f"selected_source_subject_versions={inv.selected_source_subject_versions} "
            f"destination_subjects={inv.destination_subjects} "
            f"destination_subject_versions={inv.destination_subject_versions}"
        )
        self.logger.info(
            f"[{source}] totals_since_task_start: "
            f"subject_versions_changed={totals.subject_versions_changed} "
            f"compatibility_configs_changed={totals.compatibility_configs_changed} "
            f"modes_changed={totals.modes_changed} "
            f"unsupported_features_removed={totals.unsupported_features_removed} "
            f"errors={totals.errors}"
        )

    def _verify_rpk_status(self, expected_subjects: int, expected_versions: int):
        # `rpk shadow status --format json` renders the same admin-v2
        # SchemaRegistrySyncStatus; assert it agrees with the admin API.
        rpk = RpkTool(self.target_cluster_service)
        status = rpk.shadow_status(LINK_NAME)
        self.logger.info(f"rpk shadow status: {json.dumps(status)}")
        sr = status["schema_registry"]
        inv = sr["inventory"]
        totals = sr["totals_since_task_start"]
        assert inv["selected_source_subjects"] == expected_subjects, status
        assert inv["selected_source_subject_versions"] == expected_versions, status
        assert inv["destination_subjects"] == expected_subjects, status
        assert inv["destination_subject_versions"] == expected_versions, status
        assert totals["subject_versions_changed"] == expected_versions, status
        assert totals["errors"] == 0, status
        for name in self.EXPECTED_ZERO_COUNTERS:
            assert totals[name] == 0, status

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_e2e(self):
        src = SchemaRegistryRedpandaClient(self.source_cluster_service)
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        # The destination `_schemas` topic is NOT warmed up here on purpose:
        # creating the SR-API shadow link (below) bootstraps it, so this
        # exercises that path.

        # 1. Seed the source SR with the large reference DAG.
        initial = self._seed_initial_dag(src)

        # 2. Create a shadow link that only syncs the Schema Registry.
        source_sr_url = self._create_sr_link()
        self.logger.info(f"source SR: {source_sr_url}; seeded {len(initial)} subjects")

        # While the first full sync of the seeded DAG runs, confirm the status
        # reports an in-progress FULL sync.
        self._observe_current_full_sync()

        # 3. SCENARIO A -- additions racing an in-flight sync. Immediately after
        #    creating the link (the first full sync is now running), register a
        #    second wave of leaves + mids that reference them. Whether they are
        #    enumerated by the current sync or the next one, they must all land.
        mid_sync: list[Pair] = []
        for i in range(self.LEAVES, self.LEAVES + self.EXTRA_LEAVES):
            mid_sync.append(self._add_leaf(src, i))
        for i in range(self.MIDS, self.MIDS + self.EXTRA_MIDS):
            j = i - self.MIDS
            la = self.LEAVES + (2 * j) % self.EXTRA_LEAVES
            lb = self.LEAVES + (2 * j + 1) % self.EXTRA_LEAVES
            mid_sync.append(self._add_mid(src, i, la, lb))

        # Wait for the initial DAG and the mid-sync additions to all replicate,
        # with bodies, references, and IDs matching the source.
        self._wait_synced(src, dest, initial + mid_sync)
        self.logger.info(
            f"initial + mid-sync waves synced ({len(initial) + len(mid_sync)} subjects)"
        )

        # 4. SCENARIO B -- additions after a completed sync, picked up by the
        #    next full sync. Add a new backward-compatible version of an existing
        #    subject, a brand-new standalone subject, and new tops referencing
        #    already-synced mids.
        after_sync: list[Pair] = []
        after_sync.append(self._add_leaf(src, 0, evolve=True, version=2))
        self._register(
            src, "late-value", self._record("Late", [{"name": "n", "type": "int"}])
        )
        after_sync.append(("late-value", 1))
        for i in range(self.TOPS, self.TOPS + 4):
            after_sync.append(
                self._add_top(src, i, (2 * i) % self.MIDS, (2 * i + 1) % self.MIDS)
            )

        # Re-verify leaf-0 v1 alongside its new v2: a backward-compatible new
        # version must not clobber the original. Kept out of all_pairs below so
        # the counter totals are not double-counted.
        self._wait_synced(src, dest, after_sync + [("leaf-0-value", 1)])
        self.logger.info("post-sync additions replicated to destination")

        # 5. Sanity-check the destination subject inventory covers every wave.
        all_pairs = initial + mid_sync + after_sync
        all_subjects = {s for s, _ in all_pairs}
        dest_subjects = set(dest.get_subjects().json())
        # Exact equality (not subset): the destination must hold the synced
        # subjects and nothing more -- catches spurious or duplicated imports.
        assert dest_subjects == all_subjects, (
            f"missing: {all_subjects - dest_subjects}; "
            f"unexpected: {dest_subjects - all_subjects}"
        )

        # 6. Surface and verify the Schema Registry sync status counters, both
        #    through the shadow-link admin API and the rpk `shadow status`
        #    rendering of it.
        self._verify_counters(all_pairs)

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_soft_delete(self):
        src = SchemaRegistryRedpandaClient(self.source_cluster_service)
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        # seeded-value: mixed active/deleted before the first sync.
        # after-value: version-level delete after a sync.
        # whole-value: subject-level delete after a sync (source's
        #   active-only listing then 404s).
        seeded, after, whole = "seeded-value", "after-value", "whole-value"

        def _v1(name: str) -> dict:
            return self._record(name, [{"name": "v", "type": "string"}])

        def _v2(name: str) -> dict:
            # Backward-compatible evolution (added field with a default).
            return self._record(
                name,
                [
                    {"name": "v", "type": "string"},
                    {"name": "e", "type": "long", "default": 0},
                ],
            )

        self._register(src, seeded, _v1("Seeded"))
        self._register(src, seeded, _v2("Seeded"))
        assert src.delete_subject_version(seeded, "2").status_code == 200

        self._register(src, after, _v1("After"))
        self._register(src, after, _v2("After"))

        self._register(src, whole, _v1("Whole"))

        self._create_sr_link()

        subjects = [seeded, after, whole]
        self._wait_delete_synced(src, dest, subjects)

        assert src.delete_subject_version(after, "2").status_code == 200
        assert src.delete_subject(whole).status_code == 200
        self._wait_delete_synced(src, dest, subjects)

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_compatibility(self):
        # A subject-level compatibility override round-tripping through the
        # destination's write API -- a path the reconciler unit fakes cannot
        # cover -- then un-setting it so the destination reverts to the
        # inherited default. Mode replication is covered e2e by
        # test_schema_registry_api_sync_context_remap.
        src = SchemaRegistryRedpandaClient(self.source_cluster_service)
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        keep = "keep-value"
        schema = self._record("V", [{"name": "v", "type": "string"}])
        self._register(src, keep, schema)
        # A subject-level compatibility override on the source, to be mirrored.
        assert (
            src.set_config_subject(
                keep, data=json.dumps({"compatibility": "FULL"})
            ).status_code
            == 200
        )

        self._create_sr_link()
        self._wait_synced(src, dest, [(keep, 1)])

        # The compatibility override replicates to the destination and moves the
        # compatibility_configs_changed counter.
        def config_synced() -> bool:
            c = dest.get_config_subject(keep)
            return c.status_code == 200 and c.json().get("compatibilityLevel") == "FULL"

        wait_until(
            config_synced,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="compatibility config did not replicate to the destination",
        )
        wait_until(
            lambda: self._admin_sr_status().totals_since_task_start.compatibility_configs_changed
            >= 1,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="compatibility_configs_changed counter did not advance",
        )

        # Un-setting the source override propagates as a delete, reverting the
        # destination subject to the inherited (global) default rather than
        # leaving the stale FULL override behind.
        before = self._admin_sr_status().totals_since_task_start.compatibility_configs_changed
        assert src.delete_config_subject(keep).status_code == 200

        def config_delete_synced() -> bool:
            # With no subject-level override remaining, a non-fallback GET on the
            # destination reports it missing (40401); a fallback GET now yields
            # the global default rather than the removed FULL override.
            missing = dest.get_config_subject(keep).status_code == 404
            fallback = dest.get_config_subject(keep, fallback=True)
            reverted = (
                fallback.status_code == 200
                and fallback.json().get("compatibilityLevel") != "FULL"
            )
            return missing and reverted

        wait_until(
            config_delete_synced,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="config override deletion did not replicate to the destination",
        )
        wait_until(
            lambda: self._admin_sr_status().totals_since_task_start.compatibility_configs_changed
            > before,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="compatibility_configs_changed counter did not advance for the delete",
        )

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_hard_delete(self):
        # A source hard-delete (which requires a soft-delete first on the
        # destination) being propagated as a purge -- a path the reconciler unit
        # fakes cannot cover.
        src = SchemaRegistryRedpandaClient(self.source_cluster_service)
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        keep, gone = "keep-value", "gone-value"
        schema = self._record("V", [{"name": "v", "type": "string"}])
        self._register(src, keep, schema)
        self._register(src, gone, schema)

        self._create_sr_link()
        self._wait_synced(src, dest, [(keep, 1), (gone, 1)])

        # Hard-delete gone-value at the source (soft then permanent). The next
        # full sync must purge it from the destination, soft-deleting the still
        # active destination version before the permanent delete.
        assert src.delete_subject(gone).status_code == 200
        assert src.delete_subject(gone, permanent=True).status_code == 200

        def gone_purged() -> bool:
            active, deleted = self._version_state(dest, gone)
            return not active and not deleted

        wait_until(
            gone_purged,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="source hard-delete was not propagated to the destination",
        )

        # The in-source subject is untouched by the purge.
        assert self._schema_view(dest, keep, 1) is not None

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_out_of_scope_reference(self):
        src = SchemaRegistryRedpandaClient(self.source_cluster_service)
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        # leaf-0 is a referent; mid-0 references it; ok-value is independent.
        self._add_leaf(src, 0)
        self._add_mid(src, 0, 0, 0)
        self._register(
            src, "ok-value", self._record("Ok", [{"name": "v", "type": "string"}])
        )

        # Select the referrer and the independent subject but NOT the referent,
        # so mid-0's reference is out of scope. Importing mid-0 must fail and be
        # counted, while ok-value still syncs and the full sync completes (the
        # link neither parks nor faults).
        self._create_sr_link(source_filter_subjects=["mid-0-value", "ok-value"])

        self._wait_synced(src, dest, [("ok-value", 1)])

        # last_full_sync.errors >= 1 proves the error was counted within a
        # completed full sync (best-effort), not that the task parked/faulted.
        def errored() -> bool:
            sr = self._admin_sr_status()
            return (
                sr.HasField("last_full_sync")
                and sr.last_full_sync.errors >= 1
                and sr.totals_since_task_start.errors >= 1
            )

        wait_until(
            errored,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="out-of-scope reference did not surface as a counted error",
        )

        # The referrer never imported; the in-scope independent subject did.
        dest_subjects = set(dest.get_subjects().json())
        assert "ok-value" in dest_subjects, dest_subjects
        assert "mid-0-value" not in dest_subjects, dest_subjects

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_recovers_after_source_unavailable(self):
        src = SchemaRegistryRedpandaClient(self.source_cluster_service)
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        self._register(
            src,
            "orders-value",
            self._record("Orders", [{"name": "v", "type": "string"}]),
        )

        # Point the link at an endpoint the reader rejects (out-of-range port),
        # so it parks immediately -- no connect retries -- without completing a
        # sync, recording the failure in last_error_message.
        self._create_sr_link(source_url="http://127.0.0.1:99999")

        # The park records the failure in last_error_message and, having
        # completed no sync, leaves last_full_sync unset.
        def parked() -> bool:
            sr = self._admin_sr_status()
            return sr.last_error_message != "" and not sr.HasField("last_full_sync")

        wait_until(
            parked,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="link did not park on an unreachable source",
        )

        # Repoint the link at the real source. The config change forces a fresh
        # full sync, which now reaches the source and replicates.
        good_url = self.source_cluster_service.schema_reg(limit=1)
        link = self.get_link(LINK_NAME)
        link.configurations.schema_registry_sync_options.shadow_schema_registry_api.source_url = good_url
        self.update_link(
            shadow_link=link,
            update_mask=google.protobuf.field_mask_pb2.FieldMask(
                paths=["configurations.schema_registry_sync_options"]
            ),
        )

        self._wait_synced(src, dest, [("orders-value", 1)])

        def recovered() -> bool:
            sr = self._admin_sr_status()
            return (
                sr.HasField("last_full_sync")
                and sr.inventory.selected_source_subjects == 1
                and sr.totals_since_task_start.subject_versions_changed >= 1
            )

        wait_until(
            recovered,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="link did not recover after repointing at a reachable source",
        )

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_memory_backpressure(self):
        src = SchemaRegistryRedpandaClient(self.source_cluster_service)
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        # Shrink the reconcile memory budget to its 1 MiB floor and raise
        # parallelism so many bodies are fetched at once; their combined size
        # exceeds the budget, forcing the byte-budget semaphore to serialize.
        # Each body stays under the 200 KiB per-allocation cap the ducktape log
        # checker enforces, so no single import trips an oversized-allocation
        # failure while the aggregate still exercises backpressure.
        self.target_cluster_service.set_cluster_config(
            {
                "schema_registry_sync_memory_bytes": 1024 * 1024,
                "schema_registry_sync_parallelism": 14,
            }
        )

        # ~100 KiB per body x 16 subjects: 14 fetched concurrently is ~1.4 MiB,
        # above the 1 MiB budget.
        body = 100 * 1024
        pairs: list[Pair] = []
        for i in range(16):
            subject = f"big-{i}-value"
            self._register(src, subject, self._large_schema(i, body))
            pairs.append((subject, 1))

        self._create_sr_link()
        self._wait_synced(src, dest, pairs)

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_survives_leadership_change(self):
        src = SchemaRegistryRedpandaClient(self.source_cluster_service)
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)
        admin = Admin(self.target_cluster_service)

        # A small reference DAG so the first task instance does real import work.
        pairs: list[Pair] = []
        for i in range(5):
            pairs.append(self._add_leaf(src, i))
        for i in range(3):
            pairs.append(self._add_mid(src, i, (2 * i) % 5, (2 * i + 1) % 5))
        expected_versions = len(pairs)
        expected_subjects = len({s for s, _ in pairs})

        self._create_sr_link()
        self._wait_synced(src, dest, pairs)

        # Snapshot the first task instance once it has fully synced.
        def first_synced() -> bool:
            sr = self._admin_sr_status()
            return (
                sr.HasField("last_full_sync")
                and sr.inventory.destination_subject_versions == expected_versions
                and sr.totals_since_task_start.subject_versions_changed
                >= expected_versions
            )

        wait_until(
            first_synced,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="first task instance did not fully sync",
        )
        before_start = (
            self._admin_sr_status().totals_since_task_start.start_time.ToNanoseconds()
        )

        # Move _schemas/0 leadership to another destination broker. The sync
        # task follows leadership, so a fresh instance takes over there.
        info = admin.get_partitions(namespace="kafka", topic="_schemas", partition=0)
        leader = info["leader_id"]
        replicas = [r["node_id"] for r in info["replicas"]]
        assert len(replicas) > 1, f"_schemas/0 has no failover target: {info}"
        target = next(n for n in replicas if n != leader)
        admin.partition_transfer_leadership(
            namespace="kafka", topic="_schemas", partition=0, target_id=target
        )
        admin.await_stable_leader(
            topic="_schemas",
            partition=0,
            namespace="kafka",
            timeout_s=60,
            check=lambda node_id: node_id == target,
        )

        # The new instance resets its cumulative counters (documented on
        # totals_since_task_start): a fresh, later start_time and
        # subject_versions_changed back at 0. It re-derives the destination
        # inventory from the replicated _schemas log and completes a full sync
        # that imports nothing (last_full_sync == 0) -- everything is already
        # present. This relies on the destination scan syncing the local store
        # first: without it, the freshly-elected leader could see a stale view
        # of its own store, spuriously re-import already-present versions, and
        # permanently inflate subject_versions_changed for this instance (it
        # only ever accumulates, so a transient race here would never recover).
        def re_derived() -> bool:
            sr = self._admin_sr_status()
            totals = sr.totals_since_task_start
            return (
                sr.HasField("last_full_sync")
                and totals.start_time.ToNanoseconds() > before_start
                and totals.subject_versions_changed == 0
                and totals.errors == 0
                and sr.last_full_sync.subject_versions_changed == 0
                and sr.last_full_sync.errors == 0
                and sr.inventory.selected_source_subjects == expected_subjects
                and sr.inventory.destination_subject_versions == expected_versions
            )

        wait_until(
            re_derived,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="new leader did not re-derive counters after the bounce",
        )

        # The new leader keeps syncing: a subject added now is imported by it,
        # advancing the new instance's cumulative change counter.
        base_changed = (
            self._admin_sr_status().totals_since_task_start.subject_versions_changed
        )
        self._register(
            src,
            "post-bounce-value",
            self._record("PostBounce", [{"name": "v", "type": "string"}]),
        )
        self._wait_synced(src, dest, [("post-bounce-value", 1)])
        wait_until(
            lambda: self._admin_sr_status().totals_since_task_start.subject_versions_changed
            > base_changed,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="new leader did not count the post-bounce import",
        )

        # Now bounce leadership back to the original broker (A -> B -> A). Its
        # task instance ran before, was stopped on losing leadership, and is now
        # reused -- not a fresh object. stop() clears the in-memory sync state,
        # so the returning leader must look just as fresh as B did: a later
        # start_time, zeroed cumulative counters, and an immediate full sync (a
        # stale _last_full_sync would otherwise defer it up to the full-sync
        # interval). Without the reset it would report its pre-B totals instead.
        expected_subjects += 1  # post-bounce-value
        expected_versions += 1
        before_return_start = (
            self._admin_sr_status().totals_since_task_start.start_time.ToNanoseconds()
        )
        admin.partition_transfer_leadership(
            namespace="kafka", topic="_schemas", partition=0, target_id=leader
        )
        admin.await_stable_leader(
            topic="_schemas",
            partition=0,
            namespace="kafka",
            timeout_s=60,
            check=lambda node_id: node_id == leader,
        )

        def returning_leader_fresh() -> bool:
            sr = self._admin_sr_status()
            totals = sr.totals_since_task_start
            return (
                sr.HasField("last_full_sync")
                and totals.start_time.ToNanoseconds() > before_return_start
                and totals.subject_versions_changed == 0
                and totals.errors == 0
                and sr.last_full_sync.subject_versions_changed == 0
                and sr.last_full_sync.errors == 0
                and sr.inventory.selected_source_subjects == expected_subjects
                and sr.inventory.destination_subject_versions == expected_versions
            )

        wait_until(
            returning_leader_fresh,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="reused instance did not reset state on regaining leadership",
        )
