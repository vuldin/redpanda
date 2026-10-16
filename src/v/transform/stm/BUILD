load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "state_chunks",
    hdrs = ["state_chunks.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/model",
        "//src/v/serde",
        "@abseil-cpp//absl/container:flat_hash_map",
    ],
)

redpanda_cc_library(
    name = "stm",
    srcs = [
        "transform_offsets_stm.cc",
        "transform_state_stm.cc",
    ],
    hdrs = [
        "transform_offsets_stm.h",
        "transform_state_stm.h",
    ],
    implementation_deps = [
        "//src/v/cluster",
        "//src/v/cluster:errc",
        "//src/v/serde",
        "//src/v/storage:record_batch_builder",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":state_chunks",
        "//src/v/base",
        "//src/v/cluster:distributed_kv_stm",
        "//src/v/cluster:state_machine_registry",
        "//src/v/cluster:topic_table",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)
