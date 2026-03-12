#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Config builder for Consul."""

from pydantic import BaseModel, Field

CONSUL_DATA_DIR = "/consul/data"


class Ports(BaseModel):
    """Ports used in consul."""

    dns: int = Field(default=8600)
    http: int = Field(default=-1)
    https: int = Field(default=8501)
    grpc: int = Field(default=-1)
    grpc_tls: int = Field(default=-1)
    serf_lan: int = Field(default=8301)
    serf_wan: int = Field(default=8302)
    server: int = Field(default=8300)
    sidecar_min_port: int = Field(default=21000)
    sidecar_max_port: int = Field(default=21255)
    expose_min_port: int = Field(default=21500)
    expose_max_port: int = Field(default=21755)


class ConsulConfigBuilder:
    """Build the configuration file for consul."""

    def __init__(
        self,
        ports: Ports,
        datacenter: str,
        number_of_units: int,
        retry_join_addresses: list[str],
        tls_certificates: dict = None,
    ):
        self.ports = ports
        self.datacenter = datacenter
        self.number_of_units = number_of_units
        self.retry_join = retry_join_addresses
        self.tls_certificates = tls_certificates or {}

    def build(self) -> dict:
        """Build consul config file.

        Service mesh, UI, DNS, gRPC, Serf WAN are not supported
        and disabled.
        """
        # TODO: Split this separate property per parameter group
        return {
            "bind_addr": "0.0.0.0",
            "bootstrap_expect": self.number_of_units,
            "client_addr": "0.0.0.0",
            # Service mesh not supported
            "connect": {"enabled": False},
            "datacenter": self.datacenter,
            "data_dir": CONSUL_DATA_DIR,
            "ports": {
                "dns": self.ports.dns,
                "http": self.ports.http,
                "https": self.ports.https,
                "grpc": self.ports.grpc,
                "grpc_tls": self.ports.grpc_tls,
                "serf_lan": self.ports.serf_lan,
                "serf_wan": self.ports.serf_wan,
                "server": self.ports.server,
            },
            "retry_join": self.retry_join,
            "server": True,
            "tls": {
                "defaults": {
                    "verify_incoming": True,
                    "verify_outgoing": True,
                    "ca_file": self.tls_certificates.get(
                        "ca_certificate_path",
                        "/consul/config/certs/ca.pem",
                    ),
                    "cert_file": self.tls_certificates.get(
                        "server_certificate_path",
                        "/consul/config/certs/server-cert.pem",
                    ),
                    "key_file": self.tls_certificates.get(
                        "server_key_path",
                        "/consul/config/certs/server-key.pem",
                    ),
                },
            },
            # UI not enabled
            "ui_config": {"enabled": False},
        }
