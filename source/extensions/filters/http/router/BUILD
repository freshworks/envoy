load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_extension",
    "envoy_extension_package",
)

licenses(["notice"])  # Apache 2

# HTTP L7 filter responsible for routing to upstream connection pools
# Public docs: https://envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/router_filter

envoy_extension_package()

envoy_cc_extension(
    name = "config",
    srcs = ["config.cc"],
    hdrs = ["config.h"],
    extra_visibility = [
        "//test/common:__subpackages__",
        "//test/integration:__subpackages__",
    ],
    deps = [
        "//envoy/registry",
        "//source/common/router:router_lib",
        "//source/common/router:shadow_writer_lib",
        "//source/extensions/filters/http/common:factory_base_lib",
        "@envoy_api//envoy/extensions/filters/http/router/v3:pkg_cc_proto",
    ],
)
