# One image for the whole Social Network application: the C++ Thrift services
# (/usr/local/bin/<Name>Service, run from /social-network-microservices with
# config/service-config.json) and the nginx-thrift front end (OpenResty with the
# Lua scripts, generated Lua Thrift code and nginx.conf of this directory,
# started by /usr/local/bin/nginx-thrift).
#
# The C++ build stage copies only the C++ sources, so a change to the Lua
# scripts or nginx.conf does not recompile the services, and compiles through
# ccache in a BuildKit cache mount, so a change to one service recompiles only
# the translation units that changed.

FROM yg397/thrift-microservice-deps:xenial@sha256:66d016da04e1d584e73cae8826a135610c9bcbd7f338d01e86680ffda9c5614f AS builder

ARG LIB_REDIS_PLUS_PLUS_VERSION=1.2.3

# Apply patch and re-install Redis plus plus
RUN cd /tmp/redis-plus-plus\
&& sed -i '/Transaction transaction/i\\    ShardsPool* get_shards_pool(){\n        return &_pool;\n    }\n' \
   src/sw/redis++/redis_cluster.h \
&& cmake -DREDIS_PLUS_PLUS_USE_TLS=ON . \
&& make -j$(nproc) \
&& make install

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ccache \
 && rm -rf /var/lib/apt/lists/*

COPY CMakeLists.txt /social-network-microservices/CMakeLists.txt
COPY cmake/ /social-network-microservices/cmake/
COPY third_party/ /social-network-microservices/third_party/
COPY gen-cpp/ /social-network-microservices/gen-cpp/
COPY src/ /social-network-microservices/src/
ENV CCACHE_DIR=/ccache CCACHE_MAXSIZE=4G
RUN --mount=type=cache,id=social-network-ccache,target=/ccache \
    cd /social-network-microservices \
    && mkdir -p build \
    && cd build \
    && cmake -DCMAKE_BUILD_TYPE=Release \
             -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache .. \
    && make -j$(nproc) \
    && make install \
    && ccache -s \
    && mkdir -p /out/lib \
    && cp -a /usr/local/lib/*.so* /out/lib/ \
    && rm -f /out/lib/libopentracing.so*

# OpenResty (nginx + LuaJIT) with the Thrift Lua runtime, the OpenTracing
# module and the Jaeger plugin, on Ubuntu 16.04 like the services' build.
FROM yg397/openresty-thrift:xenial@sha256:6c295fda3c2f6b941d85ebf16421bafac0dcf72450c96f53a2d031b29d8c81ef

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        openssl \
        ca-certificates \
        libsasl2-2 \
        libmemcached11 \
        libmemcachedutil2 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# The services' shared libraries; libopentracing 1.5.1 is the base image's own.
COPY --from=builder /out/lib/ /usr/local/lib/
COPY --from=builder /usr/local/bin/ /usr/local/bin/
RUN ldconfig

# nginx-thrift
COPY docker/openresty-thrift/lua-thrift/ /usr/local/openresty/lualib/thrift/
COPY gen-lua/ /gen-lua/
COPY nginx-web-server/lua-scripts/ /usr/local/openresty/nginx/lua-scripts/
COPY nginx-web-server/pages/ /usr/local/openresty/nginx/pages/
COPY nginx-web-server/conf/nginx.conf nginx-web-server/conf/resolver.conf /usr/local/openresty/nginx/conf/
COPY nginx-web-server/jaeger-config.json /usr/local/openresty/nginx/jaeger-config.json
COPY nginx-web-server/nginx-thrift.sh /usr/local/bin/nginx-thrift

# The services read config/service-config.json and config/jaeger-config.yml
# relative to this directory.
COPY config/ /social-network-microservices/config/
WORKDIR /social-network-microservices
