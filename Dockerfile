# Build the runtime binary as a static executable.
# Run the compiler on the native build platform and cross-compile to the target
# arch (TARGETOS/TARGETARCH). This avoids emulating the Go toolchain under QEMU,
# which segfaults for multi-arch builds.
FROM --platform=$BUILDPLATFORM golang:1.25 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -trimpath -o /out/fkafka-runtime ./cmd/fkafka-runtime

# Minimal, non-root runtime image.
# Use the NUMERIC uid/gid (distroless "nonroot" is 65532) rather than the name,
# so a pod securityContext with runAsNonRoot:true can verify the user is
# non-root. Kubernetes cannot verify a non-numeric image user and refuses to
# start such a container (CreateContainerConfigError).
FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/fkafka-runtime /fkafka-runtime
USER 65532:65532
ENTRYPOINT ["/fkafka-runtime"]
