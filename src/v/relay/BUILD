load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = [
        "//src/v/relay:__pkg__",
        "//src/v/relay/tests:__pkg__",
    ],
)

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "fwd",
    hdrs = [
        "fwd.h",
    ],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "subscription",
    hdrs = [
        "subscription.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
    ],
)

redpanda_cc_library(
    name = "probe",
    srcs = [
        "probe.cc",
    ],
    hdrs = [
        "probe.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/metrics",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "relay",
    srcs = [
        "relay_service.cc",
        "tcp_server.cc",
        "tcp_subscription.cc",
    ],
    hdrs = [
        "relay_service.h",
        "tcp_server.h",
        "tcp_subscription.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/base",
        "//src/v/net",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        ":probe",
        ":subscription",
        "//src/v/bytes:iobuf",
        "//src/v/model",
        "//src/v/ssx:condition_variable",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)
