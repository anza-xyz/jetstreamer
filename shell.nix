{ pkgs ? import <nixpkgs> {} }:

let
  llvmPkg = pkgs.llvmPackages_16;
in
pkgs.mkShell {
  stdenv = pkgs.gcc13Stdenv;

  buildInputs = [
    # Rust toolchain
    pkgs.rustup

    # Compiler and C/C++ toolchain
    pkgs.clang_16
    llvmPkg.llvm
    llvmPkg.libclang

    # System libraries
    pkgs.zlib
    pkgs.openssl.dev
    pkgs.openssl.out
    pkgs.openssl.bin
    pkgs.libtool
    pkgs.libxml2
    pkgs.libarchive
    pkgs.systemd
    pkgs.curl
    pkgs.protobuf

    # Build tools (base-devel equivalent on Arch)
    pkgs.autoconf
    pkgs.automake
    pkgs.binutils
    pkgs.bison
    pkgs.fakeroot
    pkgs.flex
    pkgs.gawk
    pkgs.gnugrep
    pkgs.gnumake
    pkgs.gnupg
    pkgs.gnutar
    pkgs.gzip
    pkgs.m4
    pkgs.patch
    pkgs.patchelf
    pkgs.pkg-config
    pkgs.gnused
    pkgs.texinfo
    pkgs.util-linux
    pkgs.which

    # Misc dev tools
    pkgs.git
    pkgs.pkgconf
    pkgs.screen
  ];

  shellHook = ''
    export CC=clang
    export CXX=clang++
    export LIBCLANG_PATH=${llvmPkg.libclang.lib}/lib
    export LD_LIBRARY_PATH=${llvmPkg.llvm.lib}/lib:$LD_LIBRARY_PATH
    export PKG_CONFIG_PATH=${pkgs.openssl.dev}/lib/pkgconfig:${pkgs.zlib.dev}/lib/pkgconfig:$PKG_CONFIG_PATH
  '';
}
