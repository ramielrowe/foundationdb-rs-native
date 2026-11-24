# shellcheck shell=bash disable=SC2034
# Pinned inputs of the bindingtester images. Sourced by run.sh; see README.md.
#
# Everything the image is built from is pinned by an immutable identifier:
#   * the upstream bindingtester and the Python tester come from `git archive` of the exact
#     release commit (FDB_COMMIT; the commits the 7.4.10 and 8.0.1 tags point to),
#   * the Python `fdb` package is the official PyPI wheel (WHEEL_URL) verified against
#     WHEEL_SHA256 (it is the same code as bindings/python/fdb of that commit plus the generated
#     fdboptions.py and apiversion.py, which are not in git),
#   * libfdb_c.so is copied from the official foundationdb/foundationdb image, referenced by
#     digest (FDB_IMAGE); LIBFDB_C_SHA256 is the library inside that image and the build verifies it,
#   * the same FDB_IMAGE is the server under test, and the base image is pinned by digest too.

BT_BASE_IMAGE="ubuntu:26.04@sha256:f144425ff09be612d6d9ad965196e9cdc23dae1f42110a8a11a3e9a8198759f7"

# bt_pins <7.4|8.0>: sets FDB_VERSION, FDB_COMMIT, FDB_IMAGE, WHEEL_URL, WHEEL_SHA256,
# LIBFDB_C_SHA256 and FDB_API_VERSION. Returns 1 for an unknown version.
bt_pins() {
  case "$1" in
    7.4)
      FDB_VERSION=7.4.10
      FDB_COMMIT=eed7ac4e51743afe22255d9f42fd6abfd34161ef
      FDB_IMAGE=foundationdb/foundationdb:7.4.10@sha256:c75fa3531b9f8c0bda320645f1583a8376fd276f7fd060307d252111d29c3bce
      WHEEL_URL=https://files.pythonhosted.org/packages/e3/4f/81e53f60b668ea4c41ca80e6457c9ab42e96237b415aac81d0423f83cc4b/foundationdb-7.4.10-py3-none-any.whl
      WHEEL_SHA256=df1e81649c554d2dbba0e866b29491c9366a0a3996edf6df06782e88b909d856
      LIBFDB_C_SHA256=0a78338ba7897b1fd6ef18d24d02b7b79856207f2506806cbba96e28a28c67b1
      FDB_API_VERSION=740
      # The 7.4 bindingtester clears the tenant map (special keys) before every test, which
      # fails with 2136 (tenants_disabled) unless the cluster allows tenants.
      FDB_CONFIGURE="configure new single memory tenant_mode=optional_experimental"
      ;;
    8.0)
      FDB_VERSION=8.0.1
      FDB_COMMIT=9aaa93799b2b897c518ce6ffc91d8ed129c9a6d4
      FDB_IMAGE=foundationdb/foundationdb:8.0.1@sha256:8ee19baa1a9f8232d52423971901a0e72e9910aa05f2a3130813cef86fe5e569
      WHEEL_URL=https://files.pythonhosted.org/packages/1d/52/34cd4ade74984475ebe72edbdba894e35d92807aa276324c4e1b3fb71e8c/foundationdb-8.0.1-py3-none-any.whl
      WHEEL_SHA256=c314aba0eba76471ee1bb489cf772773bde8cd776288fd1a887bdd19dc5c2c35
      LIBFDB_C_SHA256=3934e236a33297c8d3a6b7cf3768c96c79df32c7303b459b48fd9183fe85f8fd
      FDB_API_VERSION=800
      # Tenants were removed in 8.0 (and from its bindingtester).
      FDB_CONFIGURE="configure new single memory"
      ;;
    *)
      return 1
      ;;
  esac
}
