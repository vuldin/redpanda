load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "relay_service_test",
    timeout = "short",
    srcs = [
        "relay_service_test.cc",
    ],
    cpu = 1,
    memory = "64MiB",
    deps = [
        "//src/v/model",
        "//src/v/relay:relay",
        "//src/v/relay:subscription",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)


