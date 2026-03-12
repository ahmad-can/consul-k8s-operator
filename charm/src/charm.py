#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the application.

Consul is a tool for service discovery, service
mesh, traffic monitoring and configuration.
The charm manages the consul deployment and
day 2 operations.

The charm deploys and configures consul as a server.
Supported features: Failure detection of nodes in
the cluster.
Service mesh, service discovery, and configuration are
not yet supported.
"""

import json
import logging

from charms.consul_k8s.v0.consul_cluster import ConsulServiceProvider
from charms.observability_libs.v1.kubernetes_service_patch import KubernetesServicePatch
from charms.tls_certificates_interface.v4.tls_certificates import (
    CertificateAvailableEvent,
    CertificateRequestAttributes,
    ProviderCertificate,
    PrivateKey,
    TLSCertificatesRequiresV4,
)
from lightkube import Client
from lightkube.models.core_v1 import ServicePort
from lightkube.resources.core_v1 import Pod
from ops import main
from ops.charm import CharmBase, RelationEvent
from ops.model import ActiveStatus, BlockedStatus, Port, WaitingStatus
from ops.pebble import ChangeError, Error, Layer

from config_builder import ConsulConfigBuilder, Ports

logger = logging.getLogger(__name__)

CONSUL_CONFIG_PATH = "/consul/config/server.json"


class ConsulCharm(CharmBase):
    """Consul charm class."""

    def __init__(self, *args):
        super().__init__(*args)
        self.name = "consul"
        self.ports: Ports = self.get_consul_ports()

        self.consul = ConsulServiceProvider(charm=self)
        self.service_patch = self.open_ports()

        self.framework.observe(self.on.consul_pebble_ready, self._on_consul_pebble_ready)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.upgrade_charm, self._on_upgrade)
        self.framework.observe(self.consul.on.endpoints_request, self._on_endpoints_request)

        self._certificate_request = CertificateRequestAttributes(
            common_name=self._service_fqdn,
            sans_dns={
                self._service_fqdn,
                f"{self._service_fqdn}.cluster.local",
            },
            organization="consul",
        )
        self.certificates = TLSCertificatesRequiresV4(
            charm=self,
            relationship_name="certificates",
            certificate_requests=[self._certificate_request],
        )
        self.framework.observe(
            self.certificates.on.certificate_available, self._on_certificate_available
        )

    def get_consul_ports(self) -> Ports:
        """Return consul ports with supported values."""
        ports = {
            "dns": -1,  # Not supported
            "http": -1,  # Disable plain HTTP; use HTTPS only
            "https": 8501,
            "grpc": -1,  # Not supported
            "grpc_tls": -1,  # Not supported
            "serf_lan": 8301,
            "serf_wan": -1,  # Not supported
            "server": 8300,
            "sidecar_min_port": 0,  # Not supported
            "sidecar_max_port": 0,  # Not supported
            "expose_min_port": 0,  # Not supported
            "expose_max_port": 0,  # Not supported
        }

        if self.config.get("expose-gossip-and-rpc-ports"):
            ports["serf_lan"] = self.config.get("serflan-node-port")  # pyright: ignore

        return Ports(**ports)

    def open_ports(self) -> KubernetesServicePatch | None:
        """Open necessary service ports.

        If config expose-gossip-and-rpc-ports is not set, expose
        ports as Cluster Service ports.
        Otherwise, expose ports as Node ports using KubernetesServicePatch
        and return the object.

        Ports that are opened: serf_lan tcp/udp, http.
        """
        if not self.config.get("expose-gossip-and-rpc-ports"):
            # Expose as ClusterService
            logger.info("Creating service ports as ClusterIP")
            self.unit.set_ports(
                Port("tcp", self.ports.serf_lan),
                Port("udp", self.ports.serf_lan),
                Port("tcp", self.ports.https),
            )
            return

        # TODO: Expose RPC ports and see how cluster agent clients can use that port
        node_ports = [
            ServicePort(
                self.ports.serf_lan,
                name=f"juju-{self.ports.serf_lan}-tcp",
                protocol="TCP",
                nodePort=self.ports.serf_lan,
            ),
            ServicePort(
                self.ports.serf_lan,
                name=f"juju-{self.ports.serf_lan}-udp",
                protocol="UDP",
                nodePort=self.ports.serf_lan,
            ),
            ServicePort(
                self.ports.https,
                name=f"juju-{self.ports.https}-tcp",
                protocol="TCP",
                targetPort=self.ports.https,
            ),
        ]

        # TODO: Can we change externaltrafficpolicy, internaltrafficpolicy? need change in lib
        logger.info(f"Creating service ports as NodePort: {node_ports}")
        return KubernetesServicePatch(
            self,
            node_ports,
            service_name=f"{self.model.app.name}",
            service_type="NodePort",  # type: ignore NodePort should be added in KuberenetesServicePatch library ServiceType
            refresh_event=self.on.config_changed,
        )

    def _on_consul_pebble_ready(self, _):
        self._configure()

    def _on_config_changed(self, _):
        # TODO: Validate if serflan-node-port is in range 30000-32767
        self._configure()

    def _on_upgrade(self, _):
        self._configure()

    def _on_endpoints_request(self, event: RelationEvent):
        """Send cluster endpoints to consul client."""
        self._set_endpoints_on_related_apps(event)

    def _update_status(self, status):
        if self.unit.is_leader():
            self.app.status = status
        self.unit.status = status

    def _configure(self):
        if not self.workload.can_connect():
            self._update_status(WaitingStatus("Waiting for Pebble ready"))
            return
        if not self.model.get_relation("certificates"):
            self._update_status(BlockedStatus("Integration certificates missing"))
            return
        if not self._tls_material_ready():
            self._update_status(WaitingStatus("Waiting for TLS certificates"))
            return

        consul_config_changed = self._update_consul_config()
        pebble_layer_changed = self._update_pebble_layer()
        self._write_cli_defaults()
        restart = any([consul_config_changed, pebble_layer_changed])

        if restart:
            try:
                logger.debug("Restarting the consul service")
                self.workload.restart(self.name)
            except ChangeError as e:
                msg = f"Failed to restart Consul: {e}"
                self._update_status(BlockedStatus(msg))
                logger.error(msg)
                return

        # Send updates on cluster join addresses/datacenter to all related apps.
        self._set_endpoints_on_related_apps()
        self._update_status(ActiveStatus())

    def _update_consul_config(self) -> bool:
        datacenter: str = self.config.get("datacenter")  # pyright: ignore
        number_of_units = self.model.app.planned_units()
        join_addresses = self._get_internal_join_addresses()
        tls_certificates = {
            "ca_certificate_path": "/consul/config/certs/ca.pem",
            "server_certificate_path": "/consul/config/certs/server-cert.pem",
            "server_key_path": "/consul/config/certs/server-key.pem",
        }
        consul_config = ConsulConfigBuilder(
            self.ports, datacenter, number_of_units, join_addresses, tls_certificates
        ).build()

        if self._running_consul_config == consul_config:
            return False

        self.workload.push(CONSUL_CONFIG_PATH, json.dumps(consul_config, indent=2), make_dirs=True)
        logger.info("Consul configuration file updated")
        return True

    def _update_pebble_layer(self) -> bool:
        current_layer = self.workload.get_plan()

        if current_layer.services == self._pebble_layer.services:
            return False

        self.workload.add_layer(self.name, self._pebble_layer, combine=True)
        logger.info("Pebble layer is updated")
        return True

    def _get_hostips_for_consul_service(self, app: str, namespace: str) -> set:
        hostips = set()

        client = Client()  # pyright: ignore
        pods = client.list(Pod, namespace=namespace, labels={"app.kubernetes.io/name": app})
        for pod in pods:
            pod_status = pod.status
            if pod_status and (hostip := pod_status.hostIP):
                hostips.add(hostip)

        logger.debug("Consul pods are running on Host IPs: {hostips}")
        return hostips

    def _get_internal_join_addresses(self) -> list[str]:
        """Get consul server join addresses.

        If the consul agents are within k8s cluster, internal service dns
        name with configured serf lan port will be returned.
        Return type is list of string as this may change in future to
        return Pod IP addresses instead of cluster ip.

        Return value should be in format [<IP/dns name>:<Port>, ...]
        """
        # Return ClusterIP dns service name
        return [f"{self.model.app.name}.{self.model.name}.svc:{self.ports.serf_lan}"]

    def _get_external_join_addresses(self) -> list[str] | None:
        """Get consul server join addresses exposed at node level.

        Return Host IPs and serf lan port if expose-gossip-and-rpc-ports
        is set to True.

        Return value should be in format [<IP/dns name>:<Port>, ...]
        """
        if self.config.get("expose-gossip-and-rpc-ports"):
            ip_addresses = self._get_hostips_for_consul_service(
                self.model.app.name, self.model.name
            )
            return [f"{ip_address}:{self.ports.serf_lan}" for ip_address in ip_addresses]

        return None

    def _get_internal_http_endpoint(self) -> str:
        # Return ClusterIP dns service name (HTTPS)
        return f"{self.model.app.name}.{self.model.name}.svc:{self.ports.https}"

    def _get_exernal_http_endpoint(self) -> str | None:
        # Placeholder to send ingress endpoint once ingress relation is implemented
        return None

    def _set_endpoints_on_related_apps(self, event: RelationEvent | None = None):
        """Send cluster endpoints on the related app.

        If event is None, the cluster endpoints will be sent on all related apps.
        """
        # charm config have checks to determine if the value is string.
        # The config parameter also have default value and so datacenter
        # always return string, ignore the pyright static check.
        datacenter: str = self.config.get("datacenter")  # pyright: ignore

        internal_join_addresses = self._get_internal_join_addresses()
        external_join_addresses = self._get_external_join_addresses()
        internal_http_endpoint = self._get_internal_http_endpoint()
        external_http_endpoint = self._get_exernal_http_endpoint()

        relation = event.relation if event else None
        self.consul.set_cluster_endpoints(
            relation,
            datacenter,
            internal_join_addresses,
            external_join_addresses,
            internal_http_endpoint,
            external_http_endpoint,
        )

    def _on_certificate_available(self, _: CertificateAvailableEvent) -> None:
        if not self.workload.can_connect():
            return

        provider_certificate, private_key = self.certificates.get_assigned_certificate(
            self._certificate_request
        )
        if not provider_certificate or not private_key:
            logger.debug("Certificate event fired but certificate not available yet")
            return

        self._write_tls_assets(provider_certificate, private_key)
        self._configure()

    @property
    def _pebble_layer(self) -> Layer:
        # TODO: Add health checks
        # curl http://127.0.0.1:8500/v1/status/leader
        command = f"consul agent -config-file {CONSUL_CONFIG_PATH}"
        return Layer(
            {
                "summary": "consul layer",
                "description": "pebble config layer for the consul",
                "services": {
                    self.name: {
                        "override": "replace",
                        "summary": "consul",
                        "command": command,
                        "startup": "enabled",
                    }
                },
            }
        )

    @property
    def _running_consul_config(self) -> dict:
        """Get the on-disk Consul config."""
        if not self.workload.can_connect():
            return {}

        try:
            return json.loads(self.workload.pull(CONSUL_CONFIG_PATH, encoding="utf-8").read())
        except (FileNotFoundError, Error) as e:
            logger.error("Failed to retrieve Consul config %s", e)
            return {}

    @property
    def workload(self):
        """The main workload of the charm."""
        return self.unit.get_container(self.name)

    def _write_tls_assets(
        self,
        provider_certificate: ProviderCertificate,
        private_key: PrivateKey,
    ) -> None:
        """Write certificate, key, and CA chain into the workload container."""
        cert_dir = "/consul/config/certs"
        cert_file = f"{cert_dir}/server-cert.pem"
        key_file = f"{cert_dir}/server-key.pem"
        ca_file = f"{cert_dir}/ca.pem"

        chain_parts = [str(provider_certificate.ca)]
        chain_parts.extend(str(cert) for cert in provider_certificate.chain)
        chain_pem = "\n".join(chain_parts)

        self.workload.push(cert_file, str(provider_certificate.certificate), make_dirs=True)
        self.workload.push(key_file, str(private_key), make_dirs=True)
        self.workload.push(ca_file, chain_pem, make_dirs=True)
        logger.info("Updated Consul TLS assets")

    def _write_cli_defaults(self) -> None:
        """Write a default .consulrc so CLI works without extra env exports."""
        if not self.workload.can_connect():
            return

        address = f"https://{self._service_fqdn}:{self.ports.https}"
        cli_config = {
            "address": address,
            "ca_file": "/consul/config/certs/ca.pem",
            "cert_file": "/consul/config/certs/server-cert.pem",
            "key_file": "/consul/config/certs/server-key.pem",
            "tls_server_name": self._service_fqdn,
        }
        try:
            self.workload.push("/root/.consulrc", json.dumps(cli_config, indent=2), make_dirs=True)
            logger.info("Wrote default .consulrc for Consul CLI")
        except Error as e:
            logger.error(f"Failed to write .consulrc: {e}")

        # Single profile snippet for all shells.
        profile_snippet = "\n".join(
            [
                'export HOME="/root"',
                f'export CONSUL_HTTP_ADDR="{address}"',
                'export CONSUL_CACERT="/consul/config/certs/ca.pem"',
                'export CONSUL_CLIENT_CERT="/consul/config/certs/server-cert.pem"',
                'export CONSUL_CLIENT_KEY="/consul/config/certs/server-key.pem"',
                f'export CONSUL_TLS_SERVER_NAME="{self._service_fqdn}"',
                "",
            ]
        )
        try:
            self.workload.push(
                "/etc/profile.d/consul-cli.sh", profile_snippet, make_dirs=True, permissions=0o644
            )
            logger.info("Wrote /etc/profile.d/consul-cli.sh for Consul CLI")
        except Error as e:
            logger.error(f"Failed to write profile snippet: {e}")

        # Ensure bash shells source the snippet (login and non-login).
        try:
            marker = "## Consul CLI defaults (managed by charm)"
            bashrc = ""
            try:
                bashrc = self.workload.pull("/etc/bash.bashrc", encoding="utf-8").read()
            except FileNotFoundError:
                bashrc = ""
            if marker not in bashrc:
                new_content = "\n".join(
                    [
                        bashrc.rstrip(),
                        marker,
                        'if [ -f /etc/profile.d/consul-cli.sh ]; then',
                        "  . /etc/profile.d/consul-cli.sh",
                        "fi",
                        "",
                    ]
                )
                self.workload.push("/etc/bash.bashrc", new_content, make_dirs=True, permissions=0o644)
                logger.info("Updated /etc/bash.bashrc to source consul CLI defaults")
        except Error as e:
            logger.error(f"Failed to update /etc/bash.bashrc: {e}")

    @property
    def _service_fqdn(self) -> str:
        """Return the in-cluster DNS name for the Consul service."""
        return f"{self.model.app.name}.{self.model.name}.svc"

    def _tls_material_ready(self) -> bool:
        """Return True when certificate files are present in the workload."""
        if not self.workload.can_connect():
            return False

        cert_dir = "/consul/config/certs"
        required_files = [
            f"{cert_dir}/server-cert.pem",
            f"{cert_dir}/server-key.pem",
            f"{cert_dir}/ca.pem",
        ]

        for cert_path in required_files:
            try:
                self.workload.pull(cert_path, encoding="utf-8")
            except (FileNotFoundError, Error):
                return False
        return True


if __name__ == "__main__":
    main(ConsulCharm)
