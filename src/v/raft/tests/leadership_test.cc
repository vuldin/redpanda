// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/tests/raft_fixture.h"
#include "test_utils/async.h"

using namespace raft;
struct leadership_test_fixture : raft_fixture {
    ::testing::AssertionResult assert_single_leader() {
        auto leaders = std::ranges::count_if(
          nodes(), [](auto& node) { return node.second->raft()->is_leader(); });

        if (leaders != 1) {
            return ::testing::AssertionFailure()
                   << "Expected 1 leader, got " << leaders;
        }
        return ::testing::AssertionSuccess();
    }

    ::testing::AssertionResult assert_leadership_stable(model::node_id id) {
        ss::sleep(get_election_timeout() * 3).get();
        if (!node(id).raft()->is_leader()) {
            return ::testing::AssertionFailure()
                   << "Expected leader to be stable, previous: " << id
                   << " current: " << wait_for_leader(10s).get();
        }
        return ::testing::AssertionSuccess();
    }

    void wait_for_no_leader() {
        tests::cooperative_spin_wait_with_timeout(10s, [this] {
            return std::ranges::all_of(nodes(), [](auto& node) {
                return node.second->raft()->is_leader() == false;
            });
        }).get();
    }

    ::testing::AssertionResult assert_stable_no_leader() {
        ss::sleep(get_election_timeout() * 3).get();
        auto no_leader = std::ranges::all_of(nodes(), [](auto& node) {
            return node.second->raft()->is_leader() == false;
        });
        if (!no_leader) {
            return ::testing::AssertionFailure()
                   << "Raft group is expected to have no leader";
        }
        return ::testing::AssertionSuccess();
    }
};

TEST_F(leadership_test_fixture, test_single_node_group) {
    create_simple_group(1).get();
    auto leader_id = wait_for_leader(10s).get();

    ASSERT_TRUE(assert_single_leader());

    // leader should be stable when there are no failures
    ASSERT_TRUE(assert_leadership_stable(leader_id));
};

TEST_F(leadership_test_fixture, test_leader_is_elected_in_group) {
    create_simple_group(3).get();
    auto leader_id = wait_for_leader(10s).get();

    ASSERT_TRUE(assert_single_leader());

    // leader should be stable when there are no failures
    ASSERT_TRUE(assert_leadership_stable(leader_id));
    wait_for_committed_offset(node(leader_id).raft()->dirty_offset(), 10s)
      .get();
};

TEST_F(
  leadership_test_fixture, test_leader_is_elected_after_current_leader_fail) {
    create_simple_group(3).get();
    auto leader_id = wait_for_leader(10s).get();

    ASSERT_TRUE(assert_single_leader());

    // leader should be stable when there are no failures
    ASSERT_TRUE(assert_leadership_stable(leader_id));
    stop_node(leader_id).get();

    auto new_leader_id = wait_for_leader(10s).get();
    ASSERT_TRUE(assert_single_leader());
    // require leader id has changed
    ASSERT_NE(leader_id, new_leader_id);
    wait_for_committed_offset(node(new_leader_id).raft()->dirty_offset(), 10s)
      .get();

    ASSERT_TRUE(assert_leadership_stable(new_leader_id));
}

TEST_F(
  leadership_test_fixture,
  test_leader_is_not_elected_when_there_is_no_majority) {
    create_simple_group(3).get();
    auto leader_id = wait_for_leader(10s).get();

    ASSERT_TRUE(assert_single_leader());

    // leader should be stable when there are no failures
    ASSERT_TRUE(assert_leadership_stable(leader_id));
    stop_node(leader_id).get();

    auto new_leader_id = wait_for_leader(10s).get();
    ASSERT_TRUE(assert_single_leader());
    stop_node(new_leader_id).get();
    wait_for_no_leader();
    ss::sleep(get_election_timeout() * 3).get();
    ASSERT_TRUE(assert_stable_no_leader());
    // leader is re-elected when nodes are back up
    add_node(leader_id, model::revision_id{0});
    node(leader_id).init_and_start(all_vnodes()).get();
    leader_id = wait_for_leader(10s).get();
    ASSERT_TRUE(assert_leadership_stable(leader_id));
}

// --- pre-relinquish quiesce hook -----------------------------------------
//
// The hook exists so a layer above raft (the transform subsystem) can finish
// work bound to being leader before leadership goes away. What matters is that
// it runs on BOTH paths by which leadership is deliberately relinquished, and
// that it can never prevent the relinquishment.

TEST_F(leadership_test_fixture, quiesce_hook_runs_before_a_transfer) {
    create_simple_group(3).get();
    auto leader_id = wait_for_leader(10s).get();

    bool ran = false;
    node(leader_id).raft()->set_relinquish_quiesce_hook([&ran] {
        ran = true;
        return ss::now();
    });

    node(leader_id)
      .raft()
      ->transfer_leadership(
        transfer_leadership_request{.group = node(leader_id).raft()->group()})
      .get();

    EXPECT_TRUE(ran) << "leadership was transferred without quiescing first";
}

TEST_F(leadership_test_fixture, a_throwing_quiesce_hook_still_relinquishes) {
    // The layer above does not get a veto. If it could block a transfer by
    // failing, a maintenance drain or a decommission would wedge on a
    // subsystem that could not tidy up - strictly worse than not waiting.
    create_simple_group(3).get();
    auto leader_id = wait_for_leader(10s).get();

    bool ran = false;
    node(leader_id).raft()->set_relinquish_quiesce_hook(
      [&ran]() -> ss::future<> {
          ran = true;
          throw std::runtime_error("quiesce failed");
      });

    node(leader_id)
      .raft()
      ->transfer_leadership(
        transfer_leadership_request{.group = node(leader_id).raft()->group()})
      .get();

    EXPECT_TRUE(ran);
    // The transfer went ahead regardless: someone else leads now.
    RPTEST_REQUIRE_EVENTUALLY(
      10s, [this, leader_id] { return !node(leader_id).raft()->is_leader(); });
}

TEST_F(leadership_test_fixture, quiesce_hook_runs_when_removed_from_voters) {
    // This is the decommission shape, and the reason the hook lives in
    // consensus rather than one layer up: being removed from the voter set
    // relinquishes leadership through transfer_and_stepdown, which does NOT
    // go through do_transfer_leadership. A hook placed only on the transfer
    // path would cover maintenance and the leader balancer while silently
    // missing decommission - exactly the gap the archiver and STM prepare
    // phases already have.
    create_simple_group(3).get();
    auto leader_id = wait_for_leader(10s).get();

    bool ran = false;
    node(leader_id).raft()->set_relinquish_quiesce_hook([&ran] {
        ran = true;
        return ss::now();
    });

    std::vector<vnode> remaining;
    for (auto& [id, n] : nodes()) {
        if (id != leader_id) {
            remaining.push_back(n->get_vnode());
        }
    }
    node(leader_id)
      .raft()
      ->replace_configuration(remaining, model::revision_id(0))
      .get();

    RPTEST_REQUIRE_EVENTUALLY(10s, [&ran] { return ran; });
}
