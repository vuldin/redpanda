load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "network-slow-push_lib",
    srcs = ["transform.go"],
    importpath = "github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/testdata/network-slow-push",
    visibility = ["//visibility:private"],
    deps = ["//src/transform-sdk/go/transform"],
)

go_binary(
    name = "network-slow-push",
    out = "network-slow-push.wasm",
    embed = [":network-slow-push_lib"],
    goarch = "wasm",
    goos = "wasip1",
    visibility = ["//src/v/wasm/tests:__pkg__"],
)
