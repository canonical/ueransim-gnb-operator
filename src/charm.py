#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed operator for the 5G ueransim service."""

import json
import logging
from typing import Optional, Tuple

from charms.kubernetes_charm_libraries.v0.multus import (  # type: ignore[import]
    KubernetesMultusCharmLib,
    NetworkAnnotation,
    NetworkAttachmentDefinition,
)
from charms.observability_libs.v1.kubernetes_service_patch import (  # type: ignore[import]
    KubernetesServicePatch,
)
from charms.sdcore_amf.v0.fiveg_n2 import N2Requires  # type: ignore[import]
from charms.sdcore_gnbsim.v0.fiveg_gnb_identity import GnbIdentityProvides  # type: ignore[import]
from jinja2 import Environment, FileSystemLoader
from lightkube.core.client import Client
from lightkube.models.core_v1 import ServicePort, ServiceSpec
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Service
from ops.charm import CharmBase, CharmEvents, InstallEvent, RemoveEvent
from ops.framework import EventBase, EventSource, Handle, StoredState
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, WaitingStatus
from ops.pebble import Layer

logger = logging.getLogger(__name__)

BASE_CONFIG_PATH = "/etc"
GNB_CONFIG_FILE_NAME = "gnb.yaml"
GNB_INTERFACE_NAME = "gnb"
GNB_NETWORK_ATTACHMENT_DEFINITION_NAME = "gnb-net"
N2_RELATION_NAME = "fiveg-n2"
GNB_IDENTITY_RELATION_NAME = "fiveg_gnb_identity"
GTP_PORT=4997
DEFAULT_FIELD_MANAGER = "controller"


class NadConfigChangedEvent(EventBase):
    """Event triggered when an existing network attachment definition is changed."""

    def __init__(self, handle: Handle):
        super().__init__(handle)


class KubernetesMultusCharmEvents(CharmEvents):
    """Kubernetes Multus Charm Events."""

    nad_config_changed = EventSource(NadConfigChangedEvent)


class UERANSIMOperatorCharm(CharmBase):
    """Main class to describe juju event handling for the UE RAN simulator operator."""

    on = KubernetesMultusCharmEvents()
    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)

        self._stored.set_default(gnb_running=False)

        self._ueransim_container_name = self._ueransim_service_name = "ueransim"
        self._ueransim_container = self.unit.get_container(self._ueransim_container_name)

        self._n2_requirer = N2Requires(self, N2_RELATION_NAME)
        self._kubernetes_multus = KubernetesMultusCharmLib(
            charm=self,
            container_name=self._ueransim_container_name,
            cap_net_admin=True,
            network_annotations=[
                NetworkAnnotation(
                    name=GNB_NETWORK_ATTACHMENT_DEFINITION_NAME,
                    interface=GNB_INTERFACE_NAME,
                ),
            ],
            network_attachment_definitions_func=self._network_attachment_definitions_from_config,
            refresh_event=self.on.nad_config_changed,
        )

        self._gnb_identity_provider = GnbIdentityProvides(self, GNB_IDENTITY_RELATION_NAME)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.remove, self._on_remove)
        self.framework.observe(self.on.config_changed, self._configure)
        self.framework.observe(self.on.ueransim_pebble_ready, self._configure)
        self.framework.observe(self.on.start_radio_action, self._on_start_radio_action)
        self.framework.observe(self.on.stop_radio_action, self._on_stop_radio_action)
        self.framework.observe(self.on.fiveg_n2_relation_joined, self._configure)
        self.framework.observe(self._n2_requirer.on.n2_information_available, self._configure)
        self.framework.observe(
            self._gnb_identity_provider.on.fiveg_gnb_identity_request,
            self._on_fiveg_gnb_identity_request,
        )
        self.unit_ip = str(self.model.get_binding(N2_RELATION_NAME).network.bind_address)

    def _configure(self, event: EventBase) -> None:
        """Juju event handler.

        Sets unit status, writes ueransim configuration file and sets ip route.

        Args:
            event: Juju event
        """
        if invalid_configs := self._get_invalid_configs():
            self.unit.status = BlockedStatus(f"Configurations are invalid: {invalid_configs}")
            return
        self.on.nad_config_changed.emit()
        if not self._relation_created(N2_RELATION_NAME):
            self.unit.status = BlockedStatus("Waiting for N2 relation to be created")
            return
        if not self._ueransim_container.can_connect():
            self.unit.status = WaitingStatus("Waiting for container to be ready")
            return
        if not self._ueransim_container.exists(path=BASE_CONFIG_PATH):
            self.unit.status = WaitingStatus("Waiting for storage to be attached")
            return
        if not self._kubernetes_multus.is_ready():
            self.unit.status = WaitingStatus("Waiting for Multus to be ready")
            return

        if not self._n2_requirer.amf_hostname or not self._n2_requirer.amf_port:
            self.unit.status = WaitingStatus("Waiting for N2 information")
            return

        content = self._render_gnb_config_file(
            amf_hostname=self._n2_requirer.amf_hostname,  # type: ignore[arg-type]
            amf_port=self._n2_requirer.amf_port,  # type: ignore[arg-type]
            gnb_gtp_address=self._get_gnb_address_from_config().split("/")[0],  # type: ignore[union-attr]  # noqa: E501
            gnb_link_address=self.unit_ip,
            gnb_ngap_address=self.unit_ip,
            id_length=self._get_id_length_from_config(),  # type: ignore[arg-type]
            mcc=self._get_mcc_from_config(),  # type: ignore[arg-type]
            mnc=self._get_mnc_from_config(),  # type: ignore[arg-type]
            nci=self._get_nci_from_config(),  # type: ignore[arg-type]
            sd=self._get_sd_from_config(),  # type: ignore[arg-type]
            sst=self._get_sst_from_config(),  # type: ignore[arg-type]
            tac=self._get_tac_from_config(),  # type: ignore[arg-type]
        )
        if not self._config_file_content_matches(content=content):
            self._write_config_file(content=content)
            self._configure_ueransim_workload(restart=True)

        self._update_fiveg_gnb_identity_relation_data()
        self.unit.status = ActiveStatus()

    def _on_install(self, event: InstallEvent) -> None:
        client = Client()
        client.apply(
            Service(
                apiVersion="v1",
                kind="Service",
                metadata=ObjectMeta(
                    namespace=self.model.name,
                    name=f"{self.app.name}-external",
                    labels={
                        "app.kubernetes.io/name": self.app.name,
                    },
                ),
                spec=ServiceSpec(
                    selector={"app.kubernetes.io/name": self.app.name},
                    ports=[
                        ServicePort(name="gnb-gtp", port=GTP_PORT, protocol="UDP"),
                    ],
                    type="LoadBalancer",
                ),
            ),
            field_manager=DEFAULT_FIELD_MANAGER,
        )
        logger.info("Created/asserted existence of external gNB service")

    def _on_remove(self, event: RemoveEvent) -> None:
        client = Client()
        client.delete(
            Service,
            namespace=self.model.name,
            name=f"{self.app.name}-external",
        )
        logger.info("Removed external gNB service")

    def _on_start_radio_action(self, event: EventBase) -> None:
        logger.info("Starting radio service")
        self._configure(event)
        if not self._stored.gnb_running:
            self._ueransim_container.start(self._ueransim_service_name)
            logger.info("Started radio service")
            self._stored.gnb_running = True


    def _on_stop_radio_action(self, event: EventBase) -> None:
        logger.info("Stopping radio service")
        self._configure(event)
        if self._stored.gnb_running:
            self._ueransim_container.stop(self._ueransim_service_name)
            logger.info("Stopped radio service")
            self._stored.gnb_running = False

    def _configure_ueransim_workload(self, restart: bool = False) -> None:
        """Configures pebble layer for the gNB simulator container.

        Args:
            restart (bool): Whether to restart the ueransim container.
        """
        plan = self._ueransim_container.get_plan()
        layer = self._ueransim_pebble_layer
        if plan.services != layer.services or restart:
            self._ueransim_container.add_layer("ueransim", layer, combine=True)
            if self._stored.gnb_running:
                self._ueransim_container.restart(self._ueransim_service_name)

    def _network_attachment_definitions_from_config(self) -> list[NetworkAttachmentDefinition]:
        """Returns list of Multus NetworkAttachmentDefinitions to be created based on config."""
        gnb_nad_config = {
            "cniVersion": "0.3.1",
            "ipam": {
                "type": "static",
                "addresses": [
                    {
                        "address": self._get_gnb_address_from_config(),
                    }
                ],
            },
            "capabilities": {"mac": True},
        }
        if (gnb_interface := self._get_gnb_interface_from_config()) is not None:
            gnb_nad_config.update({"type": "macvlan", "master": gnb_interface})
        else:
            gnb_nad_config.update({"type": "bridge", "bridge": "ran-br"})
        return [
            NetworkAttachmentDefinition(
                metadata=ObjectMeta(name=GNB_NETWORK_ATTACHMENT_DEFINITION_NAME),
                spec={"config": json.dumps(gnb_nad_config)},
            ),
        ]

    def _on_fiveg_gnb_identity_request(self, event: EventBase) -> None:
        """Handles 5G GNB Identity request events.

        Args:
            event: Juju event
        """
        if not self.unit.is_leader():
            return
        self._update_fiveg_gnb_identity_relation_data()

    def _update_fiveg_gnb_identity_relation_data(self) -> None:
        """Publishes GNB name and TAC in the `fiveg_gnb_identity` relation data bag."""
        fiveg_gnb_identity_relations = self.model.relations.get(GNB_IDENTITY_RELATION_NAME)
        if not fiveg_gnb_identity_relations:
            logger.info("No %s relations found.", GNB_IDENTITY_RELATION_NAME)
            return

        tac = self._get_tac_as_int()
        if not tac:
            logger.error(
                "TAC value cannot be published on the %s relation", GNB_IDENTITY_RELATION_NAME
            )
            return
        for gnb_identity_relation in fiveg_gnb_identity_relations:
            self._gnb_identity_provider.publish_gnb_identity_information(
                relation_id=gnb_identity_relation.id, gnb_name=self._gnb_name, tac=tac
            )

    def _get_gnb_address_from_config(self) -> Optional[str]:
        return self.model.config.get("gnb-address")

    def _get_gnb_interface_from_config(self) -> Optional[str]:
        return self.model.config.get("gnb-interface")

    def _get_id_length_from_config(self) -> Optional[str]:
        return self.model.config.get("id-length")

    def _get_mcc_from_config(self) -> Optional[str]:
        return self.model.config.get("mcc")

    def _get_mnc_from_config(self) -> Optional[str]:
        return self.model.config.get("mnc")

    def _get_nci_from_config(self) -> Optional[str]:
        return self.model.config.get("nci")

    def _get_sd_from_config(self) -> Optional[str]:
        return self.model.config.get("sd")

    def _get_sst_from_config(self) -> Optional[int]:
        return int(self.model.config.get("sst"))  # type: ignore[arg-type]

    def _get_tac_from_config(self) -> Optional[str]:
        return self.model.config.get("tac")

    def _write_config_file(self, content: str) -> None:
        self._ueransim_container.push(source=content, path=f"{BASE_CONFIG_PATH}/{GNB_CONFIG_FILE_NAME}")
        logger.info(f"Config file written {BASE_CONFIG_PATH}/{GNB_CONFIG_FILE_NAME}" )

    def _gnb_config_file_is_written(self) -> bool:
        if not self._ueransim_container.exists(f"{BASE_CONFIG_PATH}/{GNB_CONFIG_FILE_NAME}"):
            return False
        return True

    def _render_gnb_config_file(
        self,
        *,
        amf_hostname: str,
        amf_port: int,
        gnb_gtp_address: str,
        gnb_link_address: str,
        gnb_ngap_address: str,
        id_length: int,
        mcc: str,
        mnc: str,
        nci: str,
        sd: str,
        sst: int,
        tac: str,
    ) -> str:
        """Renders config file based on parameters.

        Args:
            amf_hostname: AMF hostname
            amf_port: AMF port
            gnb_gtp_address: gNB's listen IP address for N3 Interface,
            gnb_link_address: gNB's listen IP address for Radio Link Simulation,
            gnb_ngap_address: gNB's bind IP address for N2 Interface,
            id_length: NR gNB ID length in bits [22...32],
            mcc: Mobile Country Code
            mnc: Mobile Network Code
            nci: NR Cell Identity
            sd: Slice ID
            sst: Slice Selection Type
            tac: Tracking Area Code

        Returns:
            str: Rendered ueransim configuration file
        """
        jinja2_env = Environment(loader=FileSystemLoader("src/templates"))
        template = jinja2_env.get_template("gnb-config.yaml.j2")
        return template.render(
            amf_hostname=amf_hostname,
            amf_port=amf_port,
            gnb_gtp_address=gnb_gtp_address,
            gnb_link_address=gnb_link_address,
            gnb_ngap_address=gnb_ngap_address,
            id_length=id_length,
            mcc=mcc,
            mnc=mnc,
            nci=nci,
            sd=sd,
            sst=sst,
            tac=tac,
        )

    def _config_file_content_matches(self, content: str) -> bool:
        """Returns whether the gnb config file content matches the provided content.

        Returns:
            bool: Whether the gnb config file content matches
        """
        if not self._ueransim_container.exists(path=f"{BASE_CONFIG_PATH}/{GNB_CONFIG_FILE_NAME}"):
            return False
        existing_content = self._ueransim_container.pull(path=f"{BASE_CONFIG_PATH}/{GNB_CONFIG_FILE_NAME}")
        if existing_content.read() != content:
            return False
        return True

    def _get_invalid_configs(self) -> list[str]:  # noqa: C901
        """Gets list of invalid Juju configurations."""
        invalid_configs = []
        if not self._get_gnb_address_from_config():
            invalid_configs.append("gnb-address")
        if not self._get_id_length_from_config():
            invalid_configs.append("id-length")
        if not self._get_mcc_from_config():
            invalid_configs.append("mcc")
        if not self._get_mnc_from_config():
            invalid_configs.append("mnc")
        if not self._get_nci_from_config():
            invalid_configs.append("nci")
        if not self._get_sd_from_config():
            invalid_configs.append("sd")
        if not self._get_sst_from_config():
            invalid_configs.append("sst")
        if not self._get_tac_from_config():
            invalid_configs.append("tac")
        return invalid_configs

    def _relation_created(self, relation_name: str) -> bool:
        """Returns whether a given Juju relation was crated.

        Args:
            relation_name (str): Relation name

        Returns:
            bool: Whether the relation was created.
        """
        return bool(self.model.relations[relation_name])

    @property
    def _gnb_name(self) -> str:
        """The gNB's name contains the model name and the app name.

        Returns:
            str: the gNB's name.
        """
        return f"{self.model.name}-ueransim-{self.app.name}"

    def _get_tac_as_int(self) -> Optional[int]:
        """Convert the TAC value in the config to an integer.

        Returns:
            TAC as an integer. None if the config value is invalid.
        """
        tac = None
        try:
            tac = int(self.model.config.get("tac"), 16)  # type: ignore[arg-type]
        except ValueError:
            logger.error("Invalid TAC value in config: it cannot be converted to integer.")
        return tac

    @property
    def _ueransim_pebble_layer(self) -> Layer:
        return Layer(
            {
                "summary": "ueransim simulator layer",
                "description": "pebble config layer for gnb simulator",
                "services": {
                    self._ueransim_service_name: {
                        "override": "replace",
                        "startup": "enabled",
                        "command": f"nr-gnb -c {BASE_CONFIG_PATH}/{GNB_CONFIG_FILE_NAME}",  # noqa: E501
                    },
                },
            }
        )


if __name__ == "__main__":  # pragma: nocover
    main(UERANSIMOperatorCharm)
